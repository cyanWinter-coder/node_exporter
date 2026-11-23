// Copyright 2015 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/user"
	"runtime"
	"slices"
	"strings"

	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/client_golang/prometheus"
	promcollectors "github.com/prometheus/client_golang/prometheus/collectors"
	versioncollector "github.com/prometheus/client_golang/prometheus/collectors/version"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/exporter-toolkit/bootstrap"

	"github.com/prometheus/node_exporter/collector"
)

// handler wraps an unfiltered http.Handler but uses a filtered handler,
// created on the fly, if filtering is requested. Create instances with
// newHandler.
type handler struct {
	unfilteredHandler http.Handler
	// manager owns the active collector generation and applies runtime
	// reloads requested through the /-/reload endpoint.
	manager *collector.Manager
	// enabledCollectors list is used for startup logging
	enabledCollectors []string
	// exporterMetricsRegistry is a separate registry for the metrics about
	// the exporter itself.
	exporterMetricsRegistry *prometheus.Registry
	includeExporterMetrics  bool
	maxRequests             int
	logger                  *slog.Logger
}

func newHandler(includeExporterMetrics bool, maxRequests int, logger *slog.Logger) *handler {
	h := &handler{
		exporterMetricsRegistry: prometheus.NewRegistry(),
		includeExporterMetrics:  includeExporterMetrics,
		maxRequests:             maxRequests,
		logger:                  logger,
	}
	if h.includeExporterMetrics {
		h.exporterMetricsRegistry.MustRegister(
			promcollectors.NewProcessCollector(promcollectors.ProcessCollectorOpts{}),
			promcollectors.NewGoCollector(),
		)
	}
	innerHandler, err := h.innerHandler()
	if err != nil {
		panic(fmt.Sprintf("Couldn't create metrics handler: %s", err))
	}
	h.unfilteredHandler = innerHandler
	return h
}

// ServeHTTP implements http.Handler.
func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	collects := r.URL.Query()["collect[]"]
	h.logger.Debug("collect query:", "collects", collects)

	excludes := r.URL.Query()["exclude[]"]
	h.logger.Debug("exclude query:", "excludes", excludes)

	if len(collects) == 0 && len(excludes) == 0 {
		// No filters, use the prepared unfiltered handler.
		h.unfilteredHandler.ServeHTTP(w, r)
		return
	}

	if len(collects) > 0 && len(excludes) > 0 {
		h.logger.Debug("rejecting combined collect and exclude queries")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Combined collect and exclude queries are not allowed."))
		return
	}

	filters := &collects
	if len(excludes) > 0 {
		// In exclude mode, filtered collectors = active - excluded.
		f := []string{}
		for _, c := range h.manager.Collectors() {
			if slices.Index(excludes, c) == -1 {
				f = append(f, c)
			}
		}
		filters = &f
	}

	// To serve filtered metrics, we create a filtering handler on the fly.
	filteredHandler, err := h.innerHandler(*filters...)
	if err != nil {
		h.logger.Warn("Couldn't create filtered metrics handler:", "err", err)
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Couldn't create filtered metrics handler: %s", err)
		return
	}
	filteredHandler.ServeHTTP(w, r)
}

// innerHandler is used to create both the one unfiltered http.Handler to be
// wrapped by the outer handler and also the filtered handlers created on the
// fly. The former is accomplished by calling innerHandler without any arguments
// (in which case it will log all the collectors enabled via command-line
// flags).
func (h *handler) innerHandler(filters ...string) (http.Handler, error) {
	var nodeCollector prometheus.Collector
	if len(filters) == 0 {
		// The unfiltered handler is created once, at startup, and owns the
		// collector manager that runtime reloads are applied to.
		if h.manager == nil {
			manager, err := collector.NewManager(h.logger)
			if err != nil {
				return nil, fmt.Errorf("couldn't create collector: %s", err)
			}
			h.manager = manager
			h.enabledCollectors = manager.Collectors()
			h.logger.Info("Enabled collectors")
			for _, c := range h.enabledCollectors {
				h.logger.Info(c)
			}
		}
		nodeCollector = h.manager
	} else {
		filtered, err := h.manager.Filtered(filters...)
		if err != nil {
			return nil, fmt.Errorf("couldn't create filtered collector: %s", err)
		}
		nodeCollector = filtered
	}

	r := prometheus.NewRegistry()
	r.MustRegister(versioncollector.NewCollector("node_exporter"))
	if err := r.Register(nodeCollector); err != nil {
		return nil, fmt.Errorf("couldn't register node collector: %s", err)
	}

	var handler http.Handler
	if h.includeExporterMetrics {
		handler = promhttp.HandlerFor(
			prometheus.Gatherers{h.exporterMetricsRegistry, r},
			promhttp.HandlerOpts{
				ErrorLog:            slog.NewLogLogger(h.logger.Handler(), slog.LevelError),
				ErrorHandling:       promhttp.ContinueOnError,
				MaxRequestsInFlight: h.maxRequests,
				Registry:            h.exporterMetricsRegistry,
			},
		)
		// Note that we have to use h.exporterMetricsRegistry here to
		// use the same promhttp metrics for all expositions.
		handler = promhttp.InstrumentMetricHandler(
			h.exporterMetricsRegistry, handler,
		)
	} else {
		handler = promhttp.HandlerFor(
			r,
			promhttp.HandlerOpts{
				ErrorLog:            slog.NewLogLogger(h.logger.Handler(), slog.LevelError),
				ErrorHandling:       promhttp.ContinueOnError,
				MaxRequestsInFlight: h.maxRequests,
			},
		)
	}

	return handler, nil
}

// reloadHandler serves runtime collector reloads. GET reports the current
// manager status; POST applies the reload described by the enable, disable
// and policy query parameters.
func (h *handler) reloadHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.writeJSON(w, http.StatusOK, h.manager.Status())
	case http.MethodPost:
		query := r.URL.Query()
		req := collector.ReloadRequest{
			Enable:  splitQueryValues(query["enable"]),
			Disable: splitQueryValues(query["disable"]),
		}
		if p := query.Get("policy"); p != "" {
			policy, err := collector.ParseDuplicatePolicy(p)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			req.Policy = &policy
		}
		result, err := h.manager.Reload(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		h.writeJSON(w, http.StatusOK, result)
	default:
		http.Error(w, "Only GET and POST requests are supported.", http.StatusMethodNotAllowed)
	}
}

// splitQueryValues flattens repeated and comma-separated query values.
func splitQueryValues(values []string) []string {
	var out []string
	for _, value := range values {
		for part := range strings.SplitSeq(value, ",") {
			if part = strings.TrimSpace(part); part != "" {
				out = append(out, part)
			}
		}
	}
	return out
}

func (h *handler) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		h.logger.Error("Couldn't encode JSON response", "err", err)
	}
}

func main() {
	var (
		disableDefaultCollectors = kingpin.Flag(
			"collector.disable-defaults",
			"Set all collectors to disabled by default.",
		).Default("false").Bool()
		maxProcs = kingpin.Flag(
			"runtime.gomaxprocs", "The target number of CPUs Go will run on (GOMAXPROCS)",
		).Envar("GOMAXPROCS").Default("1").Int()
	)

	kingpin.CommandLine.UsageWriter(os.Stdout)
	runner := bootstrap.New(bootstrap.Config{
		App:            kingpin.CommandLine,
		Name:           "node_exporter",
		Description:    "Prometheus Node Exporter",
		DefaultAddress: ":9100",
		MetricsHandlerFactory: func(b *bootstrap.Bootstrap) (http.Handler, error) {
			if *disableDefaultCollectors {
				collector.DisableDefaultCollectors()
			}
			if user, err := user.Current(); err == nil && user.Uid == "0" {
				b.Logger.Warn("Node Exporter is running as root user. This exporter is designed to run as unprivileged user, root is not required.")
			}
			runtime.GOMAXPROCS(*maxProcs)
			b.Logger.Debug("Go MAXPROCS", "procs", runtime.GOMAXPROCS(0))
			b.Handle("/debug/pprof/", http.DefaultServeMux)
			h := newHandler(!b.DisableExporterMetrics, b.MaxRequests, b.Logger)
			b.HandleFunc("/-/reload", h.reloadHandler)
			return h, nil
		},
	})

	if err := runner.Run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

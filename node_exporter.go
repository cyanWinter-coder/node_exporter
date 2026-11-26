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
	"context"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/user"
	"runtime"
	"slices"
	"sort"

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
	// enabledCollectors list is used for logging and filtering
	enabledCollectors []string
	// exporterMetricsRegistry is a separate registry for the metrics about
	// the exporter itself.
	exporterMetricsRegistry *prometheus.Registry
	includeExporterMetrics  bool
	maxRequests             int
	logger                  *slog.Logger
}

// scrapeHandler exposes a NodeCollector through promhttp while threading the
// request context into the scrape. promhttp.HandlerFor only accepts a
// context-free Gatherer, so each request gets a fresh registry whose sole
// collector invokes NodeCollector.CollectWithContext with r.Context(). This
// also guarantees that two scrapes never share the collector's metric channel
// or buffers.
type scrapeHandler struct {
	nodeCollector *collector.NodeCollector
	opts          promhttp.HandlerOpts
	gatherers     prometheus.Gatherers
	// inFlightSem, when non-nil, preserves the process-wide concurrent scrape
	// limit across the per-request promhttp handlers.
	inFlightSem chan struct{}
	// instrumentRegistry, when set, wraps each request with the standard
	// promhttp request instrumentation (equivalent to InstrumentMetricHandler).
	instrumentRegistry *prometheus.Registry
}

func (h *scrapeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	registry := prometheus.NewRegistry()
	registry.MustRegister(&ctxScrapeCollector{
		ctx:  r.Context(),
		node: h.nodeCollector,
	})

	opts := h.opts
	if h.inFlightSem != nil {
		// The shared semaphore below preserves the limit across per-request
		// promhttp handlers; promhttp's own semaphore would be request-local.
		opts.MaxRequestsInFlight = 0
	}
	gatherers := make(prometheus.Gatherers, 0, len(h.gatherers)+1)
	gatherers = append(gatherers, h.gatherers...)
	gatherers = append(gatherers, registry)
	requestHandler := promhttp.HandlerFor(gatherers, opts)

	if h.inFlightSem != nil {
		limited := requestHandler
		requestHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case h.inFlightSem <- struct{}{}:
				defer func() { <-h.inFlightSem }()
				limited.ServeHTTP(w, r)
			default:
				w.Header().Set("Content-Type", "text/plain; charset=utf-8")
				w.WriteHeader(http.StatusServiceUnavailable)
				fmt.Fprintf(w, "Limit of concurrent requests reached (%d), try again later.\n", cap(h.inFlightSem))
			}
		})
	}

	if h.instrumentRegistry != nil {
		requestHandler = promhttp.InstrumentMetricHandler(h.instrumentRegistry, requestHandler)
	}
	requestHandler.ServeHTTP(w, r)
}

// ctxScrapeCollector adapts a context-aware NodeCollector to the context-free
// prometheus.Collector interface used by a registry.
type ctxScrapeCollector struct {
	ctx  context.Context
	node *collector.NodeCollector
}

func (c *ctxScrapeCollector) Describe(ch chan<- *prometheus.Desc) {
	c.node.Describe(ch)
}

func (c *ctxScrapeCollector) Collect(ch chan<- prometheus.Metric) {
	c.node.CollectWithContext(c.ctx, ch)
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
		// In exclude mode, filtered collectors = enabled - excludeed.
		f := []string{}
		for _, c := range h.enabledCollectors {
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
func (h *handler) innerHandler(filters ...string) (*scrapeHandler, error) {
	nc, err := collector.NewNodeCollector(h.logger, filters...)
	if err != nil {
		return nil, fmt.Errorf("couldn't create collector: %s", err)
	}

	// Only log the creation of an unfiltered handler, which should happen
	// only once upon startup.
	if len(filters) == 0 {
		h.logger.Info("Enabled collectors")
		for n := range nc.Collectors {
			h.enabledCollectors = append(h.enabledCollectors, n)
		}
		sort.Strings(h.enabledCollectors)
		for _, c := range h.enabledCollectors {
			h.logger.Info(c)
		}
	}

	// The static registry holds collectors which do not need the request
	// context; the NodeCollector itself is gathered per request instead.
	staticRegistry := prometheus.NewRegistry()
	staticRegistry.MustRegister(versioncollector.NewCollector("node_exporter"))

	gatherers := prometheus.Gatherers{staticRegistry}
	opts := promhttp.HandlerOpts{
		ErrorLog:            slog.NewLogLogger(h.logger.Handler(), slog.LevelError),
		ErrorHandling:       promhttp.ContinueOnError,
		MaxRequestsInFlight: h.maxRequests,
	}
	sh := &scrapeHandler{
		nodeCollector: nc,
		opts:          opts,
		gatherers:     gatherers,
	}
	if h.maxRequests > 0 {
		sh.inFlightSem = make(chan struct{}, h.maxRequests)
	}
	if h.includeExporterMetrics {
		sh.gatherers = append(prometheus.Gatherers{h.exporterMetricsRegistry}, sh.gatherers...)
		sh.opts.Registry = h.exporterMetricsRegistry
		// Note that we have to use h.exporterMetricsRegistry here to
		// use the same promhttp metrics for all expositions.
		sh.instrumentRegistry = h.exporterMetricsRegistry
	}

	return sh, nil
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
			return newHandler(!b.DisableExporterMetrics, b.MaxRequests, b.Logger), nil
		},
	})

	if err := runner.Run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// Copyright 2026 The Prometheus Authors
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
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"

	dto "github.com/prometheus/client_model/go"
)

// errSnapshotTooLarge is returned by boundedBuffer when the buffered metrics
// response would exceed the configured size limit.
var errSnapshotTooLarge = errors.New("metrics response exceeds the configured size limit")

// boundedBuffer is an io.Writer that buffers a metrics snapshot in memory but
// refuses to grow beyond limit bytes, so a single scrape can never consume
// unbounded memory. A limit of 0 means unbounded.
type boundedBuffer struct {
	buf   bytes.Buffer
	limit int64
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if b.limit > 0 && int64(b.buf.Len())+int64(len(p)) > b.limit {
		return 0, errSnapshotTooLarge
	}
	return b.buf.Write(p)
}

// snapshotHandler serves metrics as a bounded in-memory snapshot instead of
// streaming them like promhttp.HandlerFor does. The whole response is
// gathered, validated and encoded before any response header is written, so
// clients never receive a truncated body with a 200 status when encoding
// fails halfway through. The trade-off (buffering the full response) is
// bounded by maxBytes.
type snapshotHandler struct {
	gatherer prometheus.Gatherer
	// errRegistry, if non-nil, gets the promhttp-style error counter
	// registered (used for the exporter metrics registry).
	errRegistry *prometheus.Registry
	errCnt      *prometheus.CounterVec
	maxRequests int
	maxBytes    int64
	logger      *slog.Logger
	inFlightSem chan struct{}
}

func newSnapshotHandler(gatherer prometheus.Gatherer, errRegistry *prometheus.Registry, maxRequests int, maxBytes int64, logger *slog.Logger) *snapshotHandler {
	h := &snapshotHandler{
		gatherer:    gatherer,
		errRegistry: errRegistry,
		errCnt: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "promhttp_metric_handler_errors_total",
				Help: "Total number of internal errors encountered by the promhttp metric handler.",
			},
			[]string{"cause"},
		),
		maxRequests: maxRequests,
		maxBytes:    maxBytes,
		logger:      logger,
	}
	if maxRequests > 0 {
		h.inFlightSem = make(chan struct{}, maxRequests)
	}
	// Initialize all causes that can occur below.
	h.errCnt.WithLabelValues("gathering")
	h.errCnt.WithLabelValues("encoding")
	if errRegistry != nil {
		if err := errRegistry.Register(h.errCnt); err != nil {
			// Filtered handlers are created per request and share the
			// registry; reuse the already registered counter.
			if are := (prometheus.AlreadyRegisteredError{}); errors.As(err, &are) {
				h.errCnt = are.ExistingCollector.(*prometheus.CounterVec)
			} else {
				panic(err)
			}
		}
	}
	return h
}

// ServeHTTP implements http.Handler.
//
// Failure policy: gathering errors follow the ContinueOnError semantics of
// the previous promhttp-based handler (a partial snapshot is served, or a
// 500 if nothing could be gathered). Encoding failures and oversized
// responses are detected before any header is written and result in a 500,
// never in a truncated 200 response. If the client disconnects, collection
// results and buffers are discarded as early as possible.
func (h *snapshotHandler) ServeHTTP(rsp http.ResponseWriter, req *http.Request) {
	if h.inFlightSem != nil {
		select {
		case h.inFlightSem <- struct{}{}: // All good, carry on.
			defer func() { <-h.inFlightSem }()
		default:
			http.Error(rsp, fmt.Sprintf(
				"Limit of concurrent requests reached (%d), try again later.", h.maxRequests,
			), http.StatusServiceUnavailable)
			return
		}
	}

	ctx := req.Context()
	if err := ctx.Err(); err != nil {
		h.logger.Debug("client disconnected before gathering, skipping collection", "err", err)
		return
	}

	mfs, err := h.gatherer.Gather()
	if err != nil {
		h.logger.Error("error gathering metrics", "err", err)
		h.errCnt.WithLabelValues("gathering").Inc()
		if len(mfs) == 0 {
			// Still report the error if no metrics have been gathered.
			httpError(rsp, err)
			return
		}
		// ContinueOnError: serve the partial snapshot.
	}

	contentType := expfmt.NegotiateAccept(req.Header, expfmt.DefaultAcceptedFormats()...)

	// Build metric name filter set from query params (if any). The URL can
	// be nil on hand-constructed requests.
	var metricFilter map[string]struct{}
	if req.URL != nil {
		if metricNames := req.URL.Query()["name[]"]; len(metricNames) > 0 {
			metricFilter = make(map[string]struct{}, len(metricNames))
			for _, name := range metricNames {
				metricFilter[name] = struct{}{}
			}
		}
	}

	// Encode the families in a deterministic order. Gatherers.Gather already
	// sorts by name and merges same-name families, so each family name
	// appears exactly once; sort defensively so the guarantee does not
	// depend on the Gatherer implementation.
	slices.SortFunc(mfs, func(a, b *dto.MetricFamily) int {
		return strings.Compare(a.GetName(), b.GetName())
	})

	// Encode the full snapshot into a bounded buffer before sending
	// anything, so an encoding failure can still be reported as a 500.
	buf := &boundedBuffer{limit: h.maxBytes}
	enc := expfmt.NewEncoder(buf, contentType)
	var encodeErr error
	for _, mf := range mfs {
		if metricFilter != nil {
			if _, ok := metricFilter[mf.GetName()]; !ok {
				continue
			}
		}
		if err := ctx.Err(); err != nil {
			h.logger.Debug("client disconnected during encoding, discarding snapshot", "err", err)
			return
		}
		if encodeErr = enc.Encode(mf); encodeErr != nil {
			break
		}
	}
	if encodeErr == nil {
		if closer, ok := enc.(expfmt.Closer); ok {
			// This in particular takes care of the final "# EOF\n" line
			// for OpenMetrics.
			encodeErr = closer.Close()
		}
	}
	if encodeErr != nil {
		h.errCnt.WithLabelValues("encoding").Inc()
		h.serveSnapshotError(rsp, encodeErr)
		return
	}

	payload := buf.buf.Bytes()
	compress := acceptsGzip(req.Header)
	if compress {
		// Compress into a second bounded buffer so the peak memory stays
		// limited to roughly 2*maxBytes.
		gzBuf := &boundedBuffer{limit: h.maxBytes}
		gz := gzip.NewWriter(gzBuf)
		if _, err := gz.Write(payload); err != nil {
			gz.Close()
			h.errCnt.WithLabelValues("encoding").Inc()
			h.serveSnapshotError(rsp, err)
			return
		}
		if err := gz.Close(); err != nil {
			h.errCnt.WithLabelValues("encoding").Inc()
			h.serveSnapshotError(rsp, err)
			return
		}
		payload = gzBuf.buf.Bytes()
	}

	// Final check before committing to a status code: if the client is
	// gone, drop the snapshot and free its buffers.
	if err := ctx.Err(); err != nil {
		h.logger.Debug("client disconnected before sending, discarding snapshot", "err", err)
		return
	}

	header := rsp.Header()
	header.Set("Content-Type", string(contentType))
	header.Add("Vary", "Accept")
	header.Add("Vary", "Accept-Encoding")
	if compress {
		header.Set("Content-Encoding", "gzip")
	}
	header.Set("Content-Length", strconv.Itoa(len(payload)))
	rsp.WriteHeader(http.StatusOK)
	if _, err := rsp.Write(payload); err != nil {
		h.logger.Debug("error writing metrics response", "err", err)
	}
}

// serveSnapshotError reports an encoding or size-limit failure. It is only
// called before any response header has been written.
func (h *snapshotHandler) serveSnapshotError(rsp http.ResponseWriter, err error) {
	if errors.Is(err, errSnapshotTooLarge) {
		h.logger.Error("metrics response too large", "limit_bytes", h.maxBytes)
		http.Error(rsp, fmt.Sprintf(
			"Metrics response exceeds the configured limit of %d bytes. "+
				"Reduce the number of exposed metrics or raise --web.max-response-size.\n",
			h.maxBytes,
		), http.StatusInternalServerError)
		return
	}
	h.logger.Error("error encoding metrics", "err", err)
	httpError(rsp, err)
}

// httpError mirrors promhttp's error response for gathering/encoding
// failures. It must not be called after the header or any payload has been
// sent.
func httpError(rsp http.ResponseWriter, err error) {
	rsp.Header().Del("Content-Encoding")
	http.Error(
		rsp,
		"An error has occurred while serving metrics:\n\n"+err.Error(),
		http.StatusInternalServerError,
	)
}

// acceptsGzip reports whether the client accepts gzip content encoding.
func acceptsGzip(h http.Header) bool {
	for _, v := range h.Values("Accept-Encoding") {
		for _, part := range strings.Split(v, ",") {
			token, params, _ := strings.Cut(strings.TrimSpace(part), ";")
			if token != "gzip" && token != "*" {
				continue
			}
			// A qvalue of 0 means "not acceptable".
			acceptable := true
			for p := range strings.SplitSeq(params, ";") {
				k, v, ok := strings.Cut(strings.TrimSpace(p), "=")
				if !ok || k != "q" {
					continue
				}
				if q, err := strconv.ParseFloat(v, 64); err == nil && q <= 0 {
					acceptable = false
				}
			}
			if acceptable {
				return true
			}
		}
	}
	return false
}

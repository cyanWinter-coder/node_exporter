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
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/prometheus/node_exporter/collector"
)

type uid452HTTPBlockingCollector struct {
	started     chan struct{}
	ctxCanceled chan struct{}
	once        sync.Once
}

func (c *uid452HTTPBlockingCollector) Update(ch chan<- prometheus.Metric) error {
	return c.UpdateContext(context.Background(), ch)
}

func (c *uid452HTTPBlockingCollector) UpdateContext(ctx context.Context, _ chan<- prometheus.Metric) error {
	close(c.started)
	<-ctx.Done()
	c.once.Do(func() { close(c.ctxCanceled) })
	return ctx.Err()
}

func uid452TestScrapeHandler(t *testing.T, c collector.Collector) *scrapeHandler {
	t.Helper()
	nc := collector.NewNodeCollectorWithCollectors(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		map[string]collector.Collector{"uid452_http_blocking": c},
	)
	return &scrapeHandler{
		nodeCollector: nc,
		opts:          promhttp.HandlerOpts{ErrorHandling: promhttp.ContinueOnError},
		gatherers:     prometheus.Gatherers{prometheus.NewRegistry()},
	}
}

func TestUID452ScrapeHandlerPropagatesRequestCancel(t *testing.T) {
	blocking := &uid452HTTPBlockingCollector{
		started:     make(chan struct{}),
		ctxCanceled: make(chan struct{}),
	}
	server := httptest.NewServer(uid452TestScrapeHandler(t, blocking))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	done := make(chan struct{})
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
		close(done)
	}()

	select {
	case <-blocking.started:
	case <-time.After(5 * time.Second):
		t.Fatal("blocking collector did not start")
	}

	cancel()

	select {
	case <-blocking.ctxCanceled:
	case <-time.After(5 * time.Second):
		t.Fatal("request cancellation was not propagated")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP request did not finish after cancellation")
	}
}

type uid452HTTPOkCollector struct{}

func (uid452HTTPOkCollector) Update(_ chan<- prometheus.Metric) error { return nil }

func TestUID452ScrapeHandlerServesStatusMetric(t *testing.T) {
	nc := collector.NewNodeCollectorWithCollectors(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		map[string]collector.Collector{"uid452_http_ok": uid452HTTPOkCollector{}},
	)
	h := &scrapeHandler{
		nodeCollector: nc,
		opts:          promhttp.HandlerOpts{ErrorHandling: promhttp.ContinueOnError},
		gatherers:     prometheus.Gatherers{prometheus.NewRegistry()},
	}
	server := httptest.NewServer(h)
	defer server.Close()

	resp, err := http.Get(server.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200; body:\n%s", resp.StatusCode, body)
	}
	text := string(body)
	if !strings.Contains(text, "node_scrape_collector_success") {
		t.Fatalf("response missing success metric:\n%s", text)
	}
	if !strings.Contains(text, `collector="uid452_http_ok"`) {
		t.Fatalf("response missing collector label:\n%s", text)
	}
}

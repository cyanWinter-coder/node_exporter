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
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"

	dto "github.com/prometheus/client_model/go"
)

// gathererFunc adapts a function to prometheus.Gatherer for tests.
type gathererFunc func() ([]*dto.MetricFamily, error)

func (f gathererFunc) Gather() ([]*dto.MetricFamily, error) { return f() }

func strptr(s string) *string { return &s }

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// counterFamily builds a simple counter metric family.
func counterFamily(name, help string, value float64, labels ...*dto.LabelPair) *dto.MetricFamily {
	return &dto.MetricFamily{
		Name: &name,
		Help: &help,
		Type: dto.MetricType_COUNTER.Enum(),
		Metric: []*dto.Metric{{
			Label:   labels,
			Counter: &dto.Counter{Value: &value},
		}},
	}
}

func labelPair(name, value string) *dto.LabelPair {
	return &dto.LabelPair{Name: &name, Value: &value}
}

func TestSnapshotHandlerOK(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_counter_total", Help: "A test counter."})
	reg.MustRegister(c)
	c.Add(42)

	h := newSnapshotHandler(reg, nil, 0, 1<<20, testLogger())
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	h.ServeHTTP(rec, req)

	res := rec.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", res.StatusCode, rec.Body.String())
	}
	if ct := res.Header.Get("Content-Type"); !strings.HasPrefix(ct, string(expfmt.FmtText)) {
		t.Errorf("expected text content type, got %q", ct)
	}
	if !strings.Contains(rec.Body.String(), "test_counter_total 42") {
		t.Errorf("expected metric in body, got:\n%s", rec.Body.String())
	}
	if vary := res.Header.Values("Vary"); len(vary) == 0 {
		t.Error("expected Vary header to be set")
	}
}

func TestSnapshotHandlerDeterministicOrderAndSingleFamily(t *testing.T) {
	// Two registries exposing a same-named metric family; registration
	// order is intentionally non-alphabetical.
	regA := prometheus.NewRegistry()
	regA.MustRegister(prometheus.NewCounter(prometheus.CounterOpts{Name: "b_metric_total", Help: "B."}))
	regA.MustRegister(prometheus.NewCounter(prometheus.CounterOpts{
		Name: "a_metric_total", Help: "A.", ConstLabels: prometheus.Labels{"src": "a"},
	}))
	regB := prometheus.NewRegistry()
	regB.MustRegister(prometheus.NewCounter(prometheus.CounterOpts{
		Name: "a_metric_total", Help: "A.", ConstLabels: prometheus.Labels{"src": "b"},
	}))

	h := newSnapshotHandler(prometheus.Gatherers{regB, regA}, nil, 0, 1<<20, testLogger())

	var prevBody string
	for i := range 3 {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		body := rec.Body.String()
		if rec.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d: %s", rec.Code, body)
		}
		if i > 0 && body != prevBody {
			t.Fatalf("response not deterministic across scrapes:\nfirst:\n%s\nlater:\n%s", prevBody, body)
		}
		prevBody = body
	}

	// Families must be sorted by name.
	ia := strings.Index(prevBody, "# TYPE a_metric_total")
	ib := strings.Index(prevBody, "# TYPE b_metric_total")
	if ia == -1 || ib == -1 || ia > ib {
		t.Errorf("expected families sorted by name, got:\n%s", prevBody)
	}
	// The same family name must appear exactly once even though two
	// registries expose it.
	if n := strings.Count(prevBody, "# TYPE a_metric_total"); n != 1 {
		t.Errorf("expected a_metric_total family exactly once, got %d:\n%s", n, prevBody)
	}
	// Both const-label variants must be merged into that single family.
	if !strings.Contains(prevBody, `src="a"`) || !strings.Contains(prevBody, `src="b"`) {
		t.Errorf("expected merged family to contain both label variants:\n%s", prevBody)
	}
}

func TestSnapshotHandlerEncodingFailure(t *testing.T) {
	// A gauge family whose metric carries no gauge value fails text
	// encoding. With streaming output this would truncate a 200 response;
	// the snapshot handler must fail with a 500 before sending headers.
	g := gathererFunc(func() ([]*dto.MetricFamily, error) {
		return []*dto.MetricFamily{{
			Name:   strptr("broken_metric"),
			Help:   strptr("Broken."),
			Type:   dto.MetricType_GAUGE.Enum(),
			Metric: []*dto.Metric{{}},
		}}, nil
	})

	h := newSnapshotHandler(g, nil, 0, 1<<20, testLogger())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "An error has occurred while serving metrics") {
		t.Errorf("expected recognizable error body, got:\n%s", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "# TYPE") {
		t.Errorf("response must not contain a partially encoded family:\n%s", rec.Body.String())
	}
}

func TestSnapshotHandlerGatherErrorContinues(t *testing.T) {
	// ContinueOnError semantics: a gather error with partial results still
	// serves the gathered families.
	g := gathererFunc(func() ([]*dto.MetricFamily, error) {
		return []*dto.MetricFamily{counterFamily("partial_metric_total", "Partial.", 1)},
			fmt.Errorf("collector failed")
	})
	h := newSnapshotHandler(g, nil, 0, 1<<20, testLogger())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200 for partial snapshot, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "partial_metric_total 1") {
		t.Errorf("expected partial metrics in body, got:\n%s", rec.Body.String())
	}

	// A gather error with no results at all yields a 500.
	gEmpty := gathererFunc(func() ([]*dto.MetricFamily, error) {
		return nil, fmt.Errorf("collector failed")
	})
	h = newSnapshotHandler(gEmpty, nil, 0, 1<<20, testLogger())
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d", rec.Code)
	}
}

func TestSnapshotHandlerTooLarge(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewCounter(prometheus.CounterOpts{Name: "size_test_counter_total", Help: "A test counter."}))

	h := newSnapshotHandler(reg, nil, 0, 16, testLogger())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "exceeds the configured limit") || !strings.Contains(body, "--web.max-response-size") {
		t.Errorf("expected recognizable size-limit error, got:\n%s", body)
	}
}

func TestSnapshotHandlerClientDisconnect(t *testing.T) {
	mfs := []*dto.MetricFamily{counterFamily("disconnect_metric_total", "D.", 1)}

	// Client already gone before the handler runs: nothing is gathered or
	// written.
	gathered := false
	g := gathererFunc(func() ([]*dto.MetricFamily, error) {
		gathered = true
		return mfs, nil
	})
	h := newSnapshotHandler(g, nil, 0, 1<<20, testLogger())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil).WithContext(ctx))
	if gathered {
		t.Error("collection should be skipped when the client is already disconnected")
	}
	if rec.Body.Len() != 0 || rec.Header().Get("Content-Type") != "" {
		t.Errorf("expected no response for disconnected client, got headers %v body %q", rec.Header(), rec.Body.String())
	}

	// Client disconnects after gathering: the snapshot buffer is discarded
	// instead of being written out.
	g = gathererFunc(func() ([]*dto.MetricFamily, error) {
		cancel()
		return mfs, nil
	})
	h = newSnapshotHandler(g, nil, 0, 1<<20, testLogger())
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil).WithContext(ctx))
	if rec.Body.Len() != 0 || rec.Header().Get("Content-Type") != "" {
		t.Errorf("expected snapshot to be discarded after disconnect, got headers %v body %q", rec.Header(), rec.Body.String())
	}
}

func TestSnapshotHandlerGzip(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewCounter(prometheus.CounterOpts{Name: "gzip_test_counter_total", Help: "A test counter."}))

	h := newSnapshotHandler(reg, nil, 0, 1<<20, testLogger())
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	res := rec.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", res.StatusCode)
	}
	if ce := res.Header.Get("Content-Encoding"); ce != "gzip" {
		t.Fatalf("expected gzip content encoding, got %q", ce)
	}
	gz, err := gzip.NewReader(res.Body)
	if err != nil {
		t.Fatalf("response body is not gzip: %s", err)
	}
	plain, err := io.ReadAll(gz)
	if err != nil {
		t.Fatalf("failed to decompress response: %s", err)
	}
	if !strings.Contains(string(plain), "gzip_test_counter_total") {
		t.Errorf("expected metric in decompressed body, got:\n%s", plain)
	}

	// q=0 means gzip is not acceptable.
	req = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Accept-Encoding", "gzip;q=0")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if ce := rec.Result().Header.Get("Content-Encoding"); ce != "" {
		t.Errorf("expected no content encoding for gzip;q=0, got %q", ce)
	}
}

func TestSnapshotHandlerMaxRequests(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	g := gathererFunc(func() ([]*dto.MetricFamily, error) {
		close(entered)
		<-release
		return []*dto.MetricFamily{counterFamily("blocked_metric_total", "B.", 1)}, nil
	})
	h := newSnapshotHandler(g, nil, 1, 1<<20, testLogger())

	first := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	}()

	// Wait until the first request holds the single in-flight slot.
	<-entered

	// While the first request holds the single slot, a second one is
	// rejected with 503.
	second := httptest.NewRecorder()
	h.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if second.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d", second.Code)
	}

	close(release)
	<-done
	if first.Code != http.StatusOK {
		t.Fatalf("expected status 200 for first request, got %d", first.Code)
	}
}

func TestSnapshotHandlerNameFilter(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewCounter(prometheus.CounterOpts{Name: "keep_metric_total", Help: "Keep."}))
	reg.MustRegister(prometheus.NewCounter(prometheus.CounterOpts{Name: "drop_metric_total", Help: "Drop."}))

	h := newSnapshotHandler(reg, nil, 0, 1<<20, testLogger())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics?name[]=keep_metric_total", nil))
	body := rec.Body.String()
	if !strings.Contains(body, "keep_metric_total") {
		t.Errorf("expected filtered metric in body, got:\n%s", body)
	}
	if strings.Contains(body, "drop_metric_total") {
		t.Errorf("expected unfiltered metric to be dropped, got:\n%s", body)
	}
}

func TestAcceptsGzip(t *testing.T) {
	for _, tc := range []struct {
		header string
		want   bool
	}{
		{"", false},
		{"gzip", true},
		{"identity, gzip", true},
		{"GZip", false}, // token matching is case-sensitive per registered token
		{"gzip;q=0", false},
		{"gzip; q=0.5", true},
		{"*", true},
		{"*;q=0", false},
		{"br", false},
	} {
		h := http.Header{}
		if tc.header != "" {
			h.Set("Accept-Encoding", tc.header)
		}
		if got := acceptsGzip(h); got != tc.want {
			t.Errorf("acceptsGzip(%q) = %v, want %v", tc.header, got, tc.want)
		}
	}
}

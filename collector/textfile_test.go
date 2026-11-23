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

//go:build !notextfile

package collector

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/common/promslog/flag"
)

type collectorAdapter struct {
	Collector
}

// Describe implements the prometheus.Collector interface.
func (a collectorAdapter) Describe(ch chan<- *prometheus.Desc) {
	// We have to send *some* metric in Describe, but we don't know which ones
	// we're going to get, so just send a dummy metric.
	ch <- prometheus.NewDesc("dummy_metric", "Dummy metric.", nil, nil)
}

// Collect implements the prometheus.Collector interface.
func (a collectorAdapter) Collect(ch chan<- prometheus.Metric) {
	if err := a.Update(ch); err != nil {
		panic(fmt.Sprintf("failed to update collector: %v", err))
	}
}

func TestTextfileCollector(t *testing.T) {
	tests := []struct {
		paths []string
		out   string
	}{
		{
			paths: []string{"fixtures/textfile/no_metric_files"},
			out:   "fixtures/textfile/no_metric_files.out",
		},
		{
			paths: []string{"fixtures/textfile/two_metric_files"},
			out:   "fixtures/textfile/two_metric_files.out",
		},
		{
			paths: []string{"fixtures/textfile/nonexistent_path"},
			out:   "fixtures/textfile/nonexistent_path.out",
		},
		{
			paths: []string{"fixtures/textfile/client_side_timestamp"},
			out:   "fixtures/textfile/client_side_timestamp.out",
		},
		{
			paths: []string{"fixtures/textfile/different_metric_types"},
			out:   "fixtures/textfile/different_metric_types.out",
		},
		{
			paths: []string{"fixtures/textfile/inconsistent_metrics"},
			out:   "fixtures/textfile/inconsistent_metrics.out",
		},
		{
			paths: []string{"fixtures/textfile/histogram"},
			out:   "fixtures/textfile/histogram.out",
		},
		{
			paths: []string{"fixtures/textfile/histogram_extra_dimension"},
			out:   "fixtures/textfile/histogram_extra_dimension.out",
		},
		{
			paths: []string{"fixtures/textfile/summary"},
			out:   "fixtures/textfile/summary.out",
		},
		{
			paths: []string{"fixtures/textfile/summary_extra_dimension"},
			out:   "fixtures/textfile/summary_extra_dimension.out",
		},
		{
			paths: []string{
				"fixtures/textfile/histogram_extra_dimension",
				"fixtures/textfile/summary_extra_dimension",
			},
			out: "fixtures/textfile/glob_extra_dimension.out",
		},
		{
			paths: []string{"fixtures/textfile/*_extra_dimension"},
			out:   "fixtures/textfile/glob_extra_dimension.out",
		},
		{
			paths: []string{"fixtures/textfile/metrics_merge_empty_help"},
			out:   "fixtures/textfile/metrics_merge_empty_help.out",
		},
		{
			paths: []string{"fixtures/textfile/metrics_merge_no_help"},
			out:   "fixtures/textfile/metrics_merge_no_help.out",
		},
		{
			paths: []string{"fixtures/textfile/metrics_merge_same_help"},
			out:   "fixtures/textfile/metrics_merge_same_help.out",
		},
		{
			paths: []string{"fixtures/textfile/metrics_merge_different_help"},
			out:   "fixtures/textfile/metrics_merge_different_help.out",
		},
		{
			paths: []string{"fixtures/textfile/metrics_merge_inconsistent_then_no_help"},
			out:   "fixtures/textfile/metrics_merge_inconsistent_then_no_help.out",
		},
	}

	for i, test := range tests {
		mtime := 1.0
		c := &textFileCollector{
			paths:  test.paths,
			mtime:  &mtime,
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		}

		// Suppress a log message about `nonexistent_path` not existing, this is
		// expected and clutters the test output.
		promslogConfig := &promslog.Config{}
		flag.AddFlags(kingpin.CommandLine, promslogConfig)
		if _, err := kingpin.CommandLine.Parse([]string{"--log.level", "debug"}); err != nil {
			t.Fatal(err)
		}

		registry := prometheus.NewRegistry()
		registry.MustRegister(collectorAdapter{c})

		rw := httptest.NewRecorder()
		promhttp.HandlerFor(registry, promhttp.HandlerOpts{ErrorHandling: promhttp.ContinueOnError}).ServeHTTP(rw, &http.Request{})
		got := string(rw.Body.String())

		want, err := os.ReadFile(test.out)
		if err != nil {
			t.Fatalf("%d. error reading fixture file %s: %s", i, test.out, err)
		}

		if string(want) != got {
			t.Fatalf("%d.%q want:\n\n%s\n\ngot:\n\n%s", i, test.paths, string(want), got)
		}
	}
}

func TestTextfileCollectorSupportsUTF8Names(t *testing.T) {
	c := &textFileCollector{
		paths:  []string{"fixtures/textfile/utf8_names"},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectorAdapter{c})

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("failed to gather textfile with UTF-8 names: %v", err)
	}

	for _, family := range families {
		if family.GetName() != "my.dotted.metric" {
			continue
		}
		if got := family.Metric[0].Label[0].GetName(); got != "error.message" {
			t.Fatalf("expected dotted label name, got %q", got)
		}
		return
	}
	t.Fatalf("expected dotted metric name, got %v", families)
}

// gatherTextfileMetrics collects the textfile collector once and returns the
// sample values indexed by metric family name. Test metrics carry no labels,
// so one value per family is enough; for families with multiple samples
// (e.g. mtimes) only presence is asserted.
func gatherTextfileMetrics(t *testing.T, c *textFileCollector) map[string]float64 {
	t.Helper()
	if c.logger == nil {
		c.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectorAdapter{c})

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("failed to gather textfile metrics: %v", err)
	}

	values := map[string]float64{}
	for _, mf := range families {
		for _, m := range mf.Metric {
			switch {
			case m.Gauge != nil:
				values[mf.GetName()] = m.Gauge.GetValue()
			case m.Untyped != nil:
				values[mf.GetName()] = m.Untyped.GetValue()
			case m.Counter != nil:
				values[mf.GetName()] = m.Counter.GetValue()
			}
		}
	}
	return values
}

// The following tests exercise filesystem changes (atomic replace, delete)
// between and during scrapes, which checked-in fixtures cannot express, so
// they use t.TempDir() instead of the fixtures directory.

func TestTextfileCollectorFileReplacedBetweenScrapes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "metrics.prom")
	if err := os.WriteFile(path, []byte("metric_before_replace 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	c := &textFileCollector{paths: []string{dir}}
	values := gatherTextfileMetrics(t, c)
	if _, ok := values["metric_before_replace"]; !ok {
		t.Fatalf("expected metric_before_replace before replace, got %v", values)
	}

	// Atomically replace the file, following the documented write-then-rename
	// pattern.
	tmp := filepath.Join(dir, "metrics.prom.tmp")
	if err := os.WriteFile(tmp, []byte("metric_after_replace 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}

	values = gatherTextfileMetrics(t, c)
	if _, ok := values["metric_after_replace"]; !ok {
		t.Fatalf("expected metric_after_replace after replace, got %v", values)
	}
	if _, ok := values["metric_before_replace"]; ok {
		t.Fatalf("stale metric_before_replace still exported after replace: %v", values)
	}
	if _, ok := values["node_textfile_mtime_seconds"]; !ok {
		t.Fatalf("expected mtime metric for the replaced file, got %v", values)
	}
	if got := values["node_textfile_scrape_error"]; got != 0 {
		t.Fatalf("expected no scrape error after replace, got %v", got)
	}
	if _, ok := values["node_textfile_scrape_size_limit_exceeded"]; ok {
		t.Fatal("size limit metric must not be exported when no limit is configured")
	}
}

func TestTextfileCollectorFileDeletedBetweenScrapes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "metrics.prom")
	if err := os.WriteFile(path, []byte("metric_to_delete 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	c := &textFileCollector{paths: []string{dir}}
	values := gatherTextfileMetrics(t, c)
	if _, ok := values["metric_to_delete"]; !ok {
		t.Fatalf("expected metric_to_delete before delete, got %v", values)
	}

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}

	values = gatherTextfileMetrics(t, c)
	if _, ok := values["metric_to_delete"]; ok {
		t.Fatalf("stale metric_to_delete still exported after delete: %v", values)
	}
	if _, ok := values["node_textfile_mtime_seconds"]; ok {
		t.Fatalf("stale mtime metric still exported after delete: %v", values)
	}
	if got := values["node_textfile_scrape_error"]; got != 0 {
		t.Fatalf("deleting a file between scrapes is not an error, got scrape_error=%v", got)
	}
}

func TestTextfileCollectorCorruptedFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "good.prom"), []byte("good_metric 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "corrupted.prom"), []byte("this is not a valid exposition\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	c := &textFileCollector{paths: []string{dir}}
	values := gatherTextfileMetrics(t, c)
	if _, ok := values["good_metric"]; !ok {
		t.Fatalf("corrupted file must not affect other files, got %v", values)
	}
	if got := values["node_textfile_scrape_error"]; got != 1 {
		t.Fatalf("expected scrape_error=1 for the corrupted file, got %v", got)
	}
	// Only the good file may report an mtime.
	if _, ok := values["node_textfile_mtime_seconds"]; !ok {
		t.Fatalf("expected mtime for the good file, got %v", values)
	}
}

func TestTextfileCollectorRetryOnFileChange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "metrics.prom")
	if err := os.WriteFile(path, []byte("retry_metric 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	otherPath := filepath.Join(dir, "other.txt")
	if err := os.WriteFile(otherPath, []byte("other\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	otherInfo, err := os.Stat(otherPath)
	if err != nil {
		t.Fatal(err)
	}

	// The first read races with a replace: the post-read stat reports a
	// different file. The collector must retry once and then succeed.
	calls := 0
	c := &textFileCollector{
		paths: []string{dir},
		statAfterRead: func(p string) (os.FileInfo, error) {
			calls++
			if calls == 1 {
				return otherInfo, nil
			}
			return os.Stat(p)
		},
	}
	values := gatherTextfileMetrics(t, c)
	if _, ok := values["retry_metric"]; !ok {
		t.Fatalf("expected retry_metric after one retry, got %v", values)
	}
	if got := values["node_textfile_scrape_error"]; got != 0 {
		t.Fatalf("expected no scrape error after a successful retry, got %v", got)
	}
	if calls != 2 {
		t.Fatalf("expected exactly one retry (2 post-read stats), got %d", calls)
	}

	// A file that keeps changing across the retry is skipped entirely.
	calls = 0
	c = &textFileCollector{
		paths: []string{dir},
		statAfterRead: func(string) (os.FileInfo, error) {
			calls++
			return otherInfo, nil
		},
	}
	values = gatherTextfileMetrics(t, c)
	if _, ok := values["retry_metric"]; ok {
		t.Fatalf("unstable file must not export metrics, got %v", values)
	}
	if got := values["node_textfile_scrape_error"]; got != 1 {
		t.Fatalf("expected scrape_error=1 for an unstable file, got %v", got)
	}
	if calls != 2 {
		t.Fatalf("expected the read to be retried exactly once, got %d attempts", calls)
	}
}

func TestTextfileCollectorSizeLimit(t *testing.T) {
	const limit = 128

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "small.prom"), []byte("small_metric 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Exactly at the limit: must still be parsed.
	exactContent := "#" + strings.Repeat("p", limit-len("exact_metric 1\n")-2) + "\n" + "exact_metric 1\n"
	if len(exactContent) != limit {
		t.Fatalf("broken test fixture: got %d bytes, want %d", len(exactContent), limit)
	}
	if err := os.WriteFile(filepath.Join(dir, "exact.prom"), []byte(exactContent), 0o644); err != nil {
		t.Fatal(err)
	}
	// Over the limit: must be skipped and reported.
	bigContent := "#" + strings.Repeat("p", 2*limit) + "\n" + "big_metric 1\n"
	if err := os.WriteFile(filepath.Join(dir, "big.prom"), []byte(bigContent), 0o644); err != nil {
		t.Fatal(err)
	}

	c := &textFileCollector{paths: []string{dir}, sizeLimit: limit}
	values := gatherTextfileMetrics(t, c)
	if _, ok := values["small_metric"]; !ok {
		t.Fatalf("expected small_metric below the limit, got %v", values)
	}
	if _, ok := values["exact_metric"]; !ok {
		t.Fatalf("expected exact_metric at the limit, got %v", values)
	}
	if _, ok := values["big_metric"]; ok {
		t.Fatalf("file over the size limit must be skipped, got %v", values)
	}
	if got := values["node_textfile_scrape_size_limit_exceeded"]; got != 1 {
		t.Fatalf("expected node_textfile_scrape_size_limit_exceeded=1, got %v", got)
	}
	if got := values["node_textfile_scrape_error"]; got != 1 {
		t.Fatalf("expected scrape_error=1 for the oversized file, got %v", got)
	}
}

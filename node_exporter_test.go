// Copyright 2017 The Prometheus Authors
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
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/procfs"

	"github.com/prometheus/node_exporter/collector"
)

var binary = filepath.Join(os.Getenv("GOPATH"), "bin/node_exporter")

const (
	address = "localhost:19100"
)

func TestFileDescriptorLeak(t *testing.T) {
	if _, err := os.Stat(binary); err != nil {
		t.Skipf("node_exporter binary not available, try to run `make build` first: %s", err)
	}
	fs, err := procfs.NewDefaultFS()
	if err != nil {
		t.Skipf("proc filesystem is not available, but currently required to read number of open file descriptors: %s", err)
	}
	if _, err := fs.Stat(); err != nil {
		t.Errorf("unable to read process stats: %s", err)
	}

	exporter := exec.Command(binary, "--web.listen-address", address)
	test := func(pid int) error {
		if err := queryExporter(address); err != nil {
			return err
		}
		proc, err := procfs.NewProc(pid)
		if err != nil {
			return err
		}
		fdsBefore, err := proc.FileDescriptors()
		if err != nil {
			return err
		}
		for range 5 {
			if err := queryExporter(address); err != nil {
				return err
			}
		}
		fdsAfter, err := proc.FileDescriptors()
		if err != nil {
			return err
		}
		if want, have := len(fdsBefore), len(fdsAfter); want != have {
			return fmt.Errorf("want %d open file descriptors after metrics scrape, have %d", want, have)
		}
		return nil
	}

	if err := runCommandAndTests(exporter, address, test); err != nil {
		t.Error(err)
	}
}

func TestHandlingOfDuplicatedMetrics(t *testing.T) {
	if _, err := os.Stat(binary); err != nil {
		t.Skipf("node_exporter binary not available, try to run `make build` first: %s", err)
	}

	exporter := exec.Command(binary, "--web.listen-address", address, "--collector.textfile.directory", "collector/fixtures/textfile/duplicate_metrics")
	test := func(_ int) error {
		return queryExporter(address)
	}

	if err := runCommandAndTests(exporter, address, test); err != nil {
		t.Error(err)
	}
}

func TestCollectorReloadEndpoint(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := newHandler(false, 0, logger)

	scrape := func() string {
		t.Helper()
		recorder := httptest.NewRecorder()
		h.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("want /metrics status code 200, have %d", recorder.Code)
		}
		return recorder.Body.String()
	}
	reload := func(query string) (int, string) {
		t.Helper()
		recorder := httptest.NewRecorder()
		h.reloadHandler(recorder, httptest.NewRequest(http.MethodPost, "/-/reload?"+query, nil))
		return recorder.Code, recorder.Body.String()
	}

	const cpuScrapeMetric = `node_scrape_collector_duration_seconds{collector="cpu"}`

	if before := scrape(); !strings.Contains(before, cpuScrapeMetric) {
		t.Skip("cpu collector not enabled on this platform")
	}

	// Disabling a collector stops publishing its metrics.
	code, body := reload("disable=cpu")
	if code != http.StatusOK {
		t.Fatalf("disable reload failed: code=%d body=%s", code, body)
	}
	after := scrape()
	if strings.Contains(after, cpuScrapeMetric) {
		t.Fatal("disabled cpu collector is still published")
	}
	if !strings.Contains(after, "node_collector_generation") {
		t.Fatal("collector status metrics missing from /metrics")
	}

	// The status endpoint reports the active generation.
	recorder := httptest.NewRecorder()
	h.reloadHandler(recorder, httptest.NewRequest(http.MethodGet, "/-/reload", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status request failed: code=%d", recorder.Code)
	}
	var status collector.ReloadStatus
	if err := json.Unmarshal(recorder.Body.Bytes(), &status); err != nil {
		t.Fatalf("cannot decode status: %v", err)
	}
	if !status.Reloaded || slices.Contains(status.ActiveCollectors, "cpu") {
		t.Fatalf("unexpected status after disabling cpu: %+v", status)
	}

	// Unknown collectors are rejected without touching the active generation.
	if code, _ := reload("enable=no_such_collector"); code != http.StatusBadRequest {
		t.Fatalf("want status 400 for unknown collector, have %d", code)
	}

	// Re-enabling restores the collector's metrics.
	code, body = reload("enable=cpu")
	if code != http.StatusOK {
		t.Fatalf("enable reload failed: code=%d body=%s", code, body)
	}
	var result collector.ReloadResult
	if err := json.Unmarshal([]byte(body), &result); err != nil {
		t.Fatalf("cannot decode reload result: %v", err)
	}
	if reason, excluded := result.Excluded["cpu"]; excluded {
		t.Skipf("cpu collector cannot be validated on this platform: %s", reason)
	}
	if restored := scrape(); !strings.Contains(restored, cpuScrapeMetric) {
		t.Fatal("re-enabled cpu collector is not published")
	}
}

func queryExporter(address string) error {
	resp, err := http.Get(fmt.Sprintf("http://%s/metrics", address))
	if err != nil {
		return err
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if err := resp.Body.Close(); err != nil {
		return err
	}
	if want, have := http.StatusOK, resp.StatusCode; want != have {
		return fmt.Errorf("want /metrics status code %d, have %d. Body:\n%s", want, have, b)
	}
	return nil
}

func runCommandAndTests(cmd *exec.Cmd, address string, fn func(pid int) error) error {
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start command: %s", err)
	}
	time.Sleep(50 * time.Millisecond)
	for i := range 10 {
		if err := queryExporter(address); err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
		if cmd.Process == nil || i == 9 {
			return fmt.Errorf("can't start command")
		}
	}

	errc := make(chan error)
	go func(pid int) {
		errc <- fn(pid)
	}(cmd.Process.Pid)

	err := <-errc
	if cmd.Process != nil {
		cmd.Process.Kill()
	}
	return err
}

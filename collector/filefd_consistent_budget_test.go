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

//go:build linux && !nofilefd

package collector

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/procfs"
)

type budgetClock struct {
	mu  sync.Mutex
	now time.Time
}

func newBudgetClock() *budgetClock {
	return &budgetClock{now: time.Unix(1_700_000_000, 0)}
}

func (c *budgetClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *budgetClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func budgetLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// budgetMakeProc creates a procfs-shaped <root>/<pid>/fd directory. Temporary
// directories are required here because permission bits and concurrent changes
// cannot be represented by checked-in fixtures.
func budgetMakeProc(t *testing.T, root string, pid, fds int) string {
	t.Helper()
	fdDir := filepath.Join(root, strconv.Itoa(pid), "fd")
	if err := os.MkdirAll(fdDir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", fdDir, err)
	}
	for fd := range fds {
		if err := os.Symlink("/dev/null", filepath.Join(fdDir, strconv.Itoa(fd))); err != nil {
			t.Fatalf("symlink fd %d: %v", fd, err)
		}
	}
	return fdDir
}

func budgetWriteFileNR(t *testing.T, root string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(root, "sys", "fs"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(root, "sys", "fs", "file-nr"),
		[]byte("1024\t0\t1631329\n"), 0o644,
	); err != nil {
		t.Fatal(err)
	}
}

func budgetNewScanner(t *testing.T, root string, timeout time.Duration, maxFDs int) *fdScanner {
	t.Helper()
	s, err := newFDScanner(root, timeout, maxFDs, budgetLogger())
	if err != nil {
		t.Fatalf("newFDScanner: %v", err)
	}
	return s
}

func budgetAssertAccounted(t *testing.T, snap *procFDSnapshot) {
	t.Helper()
	if got := len(snap.Counts) + len(snap.Skipped); got != len(snap.PIDs) {
		t.Fatalf("snapshot members not accounted exactly once: %d accounted, %d pids; snapshot=%+v",
			got, len(snap.PIDs), snap)
	}
}

func TestConsistentBudgetCountsSnapshot(t *testing.T) {
	root := t.TempDir()
	budgetMakeProc(t, root, 100, 0)
	budgetMakeProc(t, root, 200, 3)
	budgetMakeProc(t, root, 300, fdScanBatch+7)

	snap, err := budgetNewScanner(t, root, 0, 0).scan()
	if err != nil {
		t.Fatalf("scan: %v", err)
	}

	wantPIDs := []int{100, 200, 300}
	if len(snap.PIDs) != len(wantPIDs) {
		t.Fatalf("want PIDs %v, got %v", wantPIDs, snap.PIDs)
	}
	for i, want := range wantPIDs {
		if snap.PIDs[i] != want {
			t.Fatalf("PID %d: want %d in sorted snapshot %v", i, want, snap.PIDs)
		}
	}

	for pid, want := range map[int]int{100: 0, 200: 3, 300: fdScanBatch + 7} {
		if got := snap.Counts[pid]; got != want {
			t.Errorf("pid %d: want %d fds, got %d", pid, want, got)
		}
		if snap.Capped[pid] {
			t.Errorf("pid %d: unexpected cap marker", pid)
		}
		if reason, skipped := snap.Skipped[pid]; skipped {
			t.Errorf("pid %d: unexpectedly skipped: %q", pid, reason)
		}
	}
	budgetAssertAccounted(t, snap)
}

func TestConsistentBudgetVanishedAndPermission(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission bits are bypassed by root")
	}

	root := t.TempDir()
	survivorFD := budgetMakeProc(t, root, 100, 2)
	vanishedFD := budgetMakeProc(t, root, 200, 5)
	deniedFD := budgetMakeProc(t, root, 300, 4)

	fs, err := procfs.NewFS(root)
	if err != nil {
		t.Fatal(err)
	}
	procs, err := fs.AllProcs()
	if err != nil {
		t.Fatal(err)
	}

	scanner := budgetNewScanner(t, root, time.Second, 1024)
	// Freeze the PID set before changing the fake procfs.
	scanner.snapshot = func() (procfs.Procs, error) { return procs, nil }

	if err := os.RemoveAll(vanishedFD); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(deniedFD, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(deniedFD, 0o755) })

	snap, err := scanner.scan()
	if err != nil {
		t.Fatalf("individual process errors must not fail scan: %v", err)
	}

	if got := snap.Skipped[200]; got != fdSkipVanished {
		t.Errorf("vanished pid: want %q, got %q", fdSkipVanished, got)
	}
	if got := snap.Skipped[300]; got != fdSkipPermission {
		t.Errorf("denied pid: want %q, got %q", fdSkipPermission, got)
	}
	if got := snap.Counts[100]; got != 2 {
		t.Errorf("surviving pid: want 2, got %d", got)
	}
	budgetAssertAccounted(t, snap)

	// The survivor remains readable and the permission directory can be
	// cleaned up after the test.
	if _, err := os.Stat(survivorFD); err != nil {
		t.Errorf("survivor fd directory changed: %v", err)
	}
}

func TestConsistentBudgetPermissionClassification(t *testing.T) {
	root := t.TempDir()
	budgetMakeProc(t, root, 1, 2)
	budgetMakeProc(t, root, 2, 4)

	scanner := budgetNewScanner(t, root, time.Second, 1024)
	origOpen := scanner.openDir
	scanner.openDir = func(path string) (*os.File, error) {
		if filepath.Base(filepath.Dir(path)) == "2" {
			return nil, os.ErrPermission
		}
		return origOpen(path)
	}

	snap, err := scanner.scan()
	if err != nil {
		t.Fatalf("permission errors must be skips, not scan failures: %v", err)
	}
	if got := snap.Skipped[2]; got != fdSkipPermission {
		t.Fatalf("want %q, got %q", fdSkipPermission, got)
	}
	if got := snap.Counts[1]; got != 2 {
		t.Fatalf("pid 1 should still be counted, got %d", got)
	}
	budgetAssertAccounted(t, snap)
}

func TestConsistentBudgetTimeoutStopsFurtherOpens(t *testing.T) {
	root := t.TempDir()
	for pid := 1; pid <= 5; pid++ {
		budgetMakeProc(t, root, pid, 2)
	}

	clock := newBudgetClock()
	scanner := budgetNewScanner(t, root, time.Minute, 1024)
	scanner.now = clock.Now
	scanner.deadline = clock.Now().Add(time.Minute)

	var mu sync.Mutex
	opened := make([]int, 0, 2)
	origOpen := scanner.openDir
	scanner.openDir = func(path string) (*os.File, error) {
		pid, err := strconv.Atoi(filepath.Base(filepath.Dir(path)))
		if err != nil {
			t.Errorf("parse pid from %q: %v", path, err)
		}
		f, openErr := origOpen(path)

		mu.Lock()
		opened = append(opened, pid)
		mu.Unlock()

		// The deadline expires while pid 2 is being entered. It had already
		// started its open, but must not read its directory and later PIDs
		// must not be opened.
		if pid == 2 {
			clock.advance(2 * time.Minute)
		}
		return f, openErr
	}

	snap, err := scanner.scan()
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !snap.TimedOut {
		t.Fatal("want snapshot to record timeout")
	}
	if !scanner.timedOut {
		t.Fatal("want scanner to record timeout")
	}

	mu.Lock()
	gotOpened := append([]int(nil), opened...)
	mu.Unlock()
	if len(gotOpened) != 2 || gotOpened[0] != 1 || gotOpened[1] != 2 {
		t.Fatalf("want opens only for pids 1 and 2, got %v", gotOpened)
	}
	if _, ok := snap.Counts[2]; ok {
		t.Errorf("timed-out pid 2 must not have a count: %+v", snap.Counts)
	}
	for pid := 2; pid <= 5; pid++ {
		if got := snap.Skipped[pid]; got != fdSkipTimeout {
			t.Errorf("pid %d: want %q, got %q", pid, fdSkipTimeout, got)
		}
	}
	if got := snap.Counts[1]; got != 2 {
		t.Errorf("pid 1: want 2 fds before timeout, got %d", got)
	}
	budgetAssertAccounted(t, snap)
}

func TestConsistentBudgetPerProcessCap(t *testing.T) {
	t.Run("overflow", func(t *testing.T) {
		root := t.TempDir()
		budgetMakeProc(t, root, 100, fdScanBatch*3)
		budgetMakeProc(t, root, 200, 2)

		s := budgetNewScanner(t, root, 0, fdScanBatch)
		snap, err := s.scan()
		if err != nil {
			t.Fatal(err)
		}
		if got := snap.Counts[100]; got != fdScanBatch {
			t.Errorf("capped count: want %d, got %d", fdScanBatch, got)
		}
		if !snap.Capped[100] {
			t.Error("pid 100 must be marked capped")
		}
		if got := snap.Counts[200]; got != 2 || snap.Capped[200] {
			t.Errorf("pid 200 must be exact and uncapped, got count=%d capped=%v",
				snap.Counts[200], snap.Capped[200])
		}
		budgetAssertAccounted(t, snap)
	})

	t.Run("exact boundary", func(t *testing.T) {
		root := t.TempDir()
		budgetMakeProc(t, root, 100, fdScanBatch)

		s := budgetNewScanner(t, root, 0, fdScanBatch)
		snap, err := s.scan()
		if err != nil {
			t.Fatal(err)
		}
		if got := snap.Counts[100]; got != fdScanBatch {
			t.Errorf("boundary count: want %d, got %d", fdScanBatch, got)
		}
		if snap.Capped[100] {
			t.Error("directory ending exactly at cap must not be marked capped")
		}
	})
}

func TestConsistentBudgetConcurrentCreation(t *testing.T) {
	root := t.TempDir()
	for _, pid := range []int{10, 20, 30} {
		budgetMakeProc(t, root, pid, 2)
	}

	scanner := budgetNewScanner(t, root, time.Second, 1024)
	snapshotTaken := make(chan struct{})
	firstOpen := make(chan struct{})
	stopCreator := make(chan struct{})
	creatorDone := make(chan struct{})

	var openOnce sync.Once
	origOpen := scanner.openDir
	scanner.openDir = func(path string) (*os.File, error) {
		pid, _ := strconv.Atoi(filepath.Base(filepath.Dir(path)))
		if pid == 10 {
			openOnce.Do(func() { close(firstOpen) })
			// The snapshot has already been read before the first fd directory
			// is opened. Wait until the creator has added a post-snapshot PID.
			<-snapshotTaken
		}
		return origOpen(path)
	}

	go func() {
		<-firstOpen
		// Create one process and let the scan proceed, then keep churning.
		// Errors here can race test teardown and do not affect the frozen
		// snapshot assertion.
		_ = os.MkdirAll(filepath.Join(root, "100000", "fd"), 0o755)
		close(snapshotTaken)
		defer close(creatorDone)

		for i := 1; ; i++ {
			select {
			case <-stopCreator:
				return
			default:
				_ = os.MkdirAll(filepath.Join(root, strconv.Itoa(200_000+i), "fd"), 0o755)
			}
		}
	}()

	snap, err := scanner.scan()
	close(stopCreator)
	<-creatorDone
	if err != nil {
		t.Fatalf("scan must tolerate concurrent creation: %v", err)
	}

	if want := []int{10, 20, 30}; len(snap.PIDs) != len(want) {
		t.Fatalf("want frozen PIDs %v, got %v", want, snap.PIDs)
	}
	for i, want := range []int{10, 20, 30} {
		if snap.PIDs[i] != want {
			t.Fatalf("snapshot %v is not sorted/frozen at index %d; want %d", snap.PIDs, i, want)
		}
		if snap.PIDs[i] >= 100_000 {
			t.Fatalf("post-snapshot process leaked: %v", snap.PIDs)
		}
	}
	budgetAssertAccounted(t, snap)
}

func TestConsistentBudgetRealProc(t *testing.T) {
	if _, err := os.Stat(procfs.DefaultMountPoint); err != nil {
		t.Skipf("host procfs unavailable: %v", err)
	}

	scanner, err := newFDScanner(procfs.DefaultMountPoint, 5*time.Second, 0, budgetLogger())
	if err != nil {
		t.Skipf("cannot open host procfs: %v", err)
	}
	snap, err := scanner.scan()
	if err != nil {
		t.Fatalf("scan real procfs: %v", err)
	}
	if len(snap.PIDs) == 0 {
		t.Fatal("want at least one process on a live procfs")
	}
	budgetAssertAccounted(t, snap)

	for pid, reason := range snap.Skipped {
		switch reason {
		case fdSkipVanished, fdSkipPermission, fdSkipTimeout, fdSkipError:
		default:
			t.Errorf("pid %d: unknown stable skip reason %q", pid, reason)
		}
	}
}

type budgetCollectorAdapter struct {
	collector *fileFDStatCollector
}

func (a budgetCollectorAdapter) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range []*prometheus.Desc{
		a.collector.openFds,
		a.collector.scanTargets,
		a.collector.scanSkipped,
		a.collector.processSkipped,
		a.collector.scanTimeoutDesc,
	} {
		ch <- desc
	}
}

func (a budgetCollectorAdapter) Collect(ch chan<- prometheus.Metric) {
	if err := a.collector.Update(ch); err != nil {
		panic(err)
	}
}

func TestConsistentBudgetCollectorMetrics(t *testing.T) {
	root := t.TempDir()
	budgetWriteFileNR(t, root)
	budgetMakeProc(t, root, 100, 2)
	budgetMakeProc(t, root, 200, 0)
	vanishedFD := budgetMakeProc(t, root, 300, 9)
	if err := os.RemoveAll(vanishedFD); err != nil {
		t.Fatal(err)
	}

	source := &realProcFDSource{mountRoot: root}
	c := newFileFDStatCollector(budgetLogger(), source, true, time.Second, 1024)

	registry := prometheus.NewRegistry()
	registry.MustRegister(budgetCollectorAdapter{collector: c})
	mfs, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}

	families := make(map[string]*dto.MetricFamily, len(mfs))
	for _, mf := range mfs {
		families[mf.GetName()] = mf
	}

	if mf := families["node_filefd_process_scan_processes"]; mf == nil || len(mf.Metric) != 1 || mf.Metric[0].GetGauge().GetValue() != 3 {
		t.Fatalf("process_scan_processes must report frozen target count 3: %v", mf)
	}

	openFDs := families["node_filefd_process_open_fds"]
	if openFDs == nil || len(openFDs.Metric) != 2 {
		t.Fatalf("want two open_fds series, got %v", openFDs)
	}
	gotCounts := map[string]float64{}
	for _, m := range openFDs.Metric {
		gotCounts[budgetMetricLabel(m, "pid")] = m.GetGauge().GetValue()
	}
	if gotCounts["100"] != 2 || gotCounts["200"] != 0 {
		t.Fatalf("unexpected open fd counts: %v", gotCounts)
	}

	skipped := families["node_filefd_process_skipped"]
	if skipped == nil || len(skipped.Metric) != 1 {
		t.Fatalf("want one per-pid skipped series, got %v", skipped)
	}
	m := skipped.Metric[0]
	if budgetMetricLabel(m, "pid") != "300" || budgetMetricLabel(m, "reason") != fdSkipVanished {
		t.Fatalf("unexpected skipped metric labels: %v", m.Label)
	}

	aggregate := families["node_filefd_process_scan_skipped"]
	if aggregate == nil {
		t.Fatal("missing aggregate skipped family")
	}
	reasons := map[string]float64{}
	for _, metric := range aggregate.Metric {
		reasons[budgetMetricLabel(metric, "reason")] = metric.GetGauge().GetValue()
	}
	if reasons[fileFDSkipVanished] != 1 {
		t.Fatalf("want one aggregate vanished skip, got %v", reasons)
	}

	for _, name := range []string{"node_filefd_allocated", "node_filefd_maximum", "node_filefd_process_scan_timeout"} {
		if families[name] == nil {
			t.Errorf("missing metric family %s", name)
		}
	}
}

func budgetMetricLabel(m *dto.Metric, name string) string {
	for _, label := range m.Label {
		if label.GetName() == name {
			return label.GetValue()
		}
	}
	return ""
}

type budgetFakeSource struct {
	dir  string
	pids []int
}

func (s *budgetFakeSource) root() string { return s.dir }

func (s *budgetFakeSource) listPIDs() ([]int, error) { return s.pids, nil }

func (s *budgetFakeSource) countFDs(int, int) (int, bool, error) {
	return 0, false, os.ErrNotExist
}

func TestConsistentBudgetCollectorTimeoutOutput(t *testing.T) {
	root := t.TempDir()
	budgetWriteFileNR(t, root)
	c := newFileFDStatCollector(
		budgetLogger(), &budgetFakeSource{dir: root, pids: []int{1, 2, 3}},
		true, time.Second, 10,
	)
	base := time.Unix(1_700_000_100, 0)
	calls := 0
	c.now = func() time.Time {
		calls++
		if calls == 1 {
			return base
		}
		return base.Add(2 * time.Second)
	}

	registry := prometheus.NewRegistry()
	registry.MustRegister(budgetCollectorAdapter{collector: c})
	mfs, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	families := map[string]*dto.MetricFamily{}
	for _, mf := range mfs {
		families[mf.GetName()] = mf
	}

	if got := budgetSingleGauge(t, families, "node_filefd_process_scan_timeout"); got != 1 {
		t.Fatalf("want timeout gauge 1, got %v", got)
	}
	if got := budgetSingleGauge(t, families, "node_filefd_process_scan_processes"); got != 3 {
		t.Fatalf("want frozen target count 3, got %v", got)
	}

	var deadlineSkipped float64
	for _, m := range families["node_filefd_process_scan_skipped"].Metric {
		if budgetMetricLabel(m, "reason") == fileFDSkipDeadline {
			deadlineSkipped = m.GetGauge().GetValue()
		}
	}
	if deadlineSkipped != 3 {
		t.Fatalf("want all 3 snapshot PIDs charged to deadline, got %v", deadlineSkipped)
	}
}

func budgetSingleGauge(t *testing.T, families map[string]*dto.MetricFamily, name string) float64 {
	t.Helper()
	mf := families[name]
	if mf == nil || len(mf.Metric) != 1 {
		t.Fatalf("metric %s missing or not unary: %v", name, mf)
	}
	return mf.Metric[0].GetGauge().GetValue()
}

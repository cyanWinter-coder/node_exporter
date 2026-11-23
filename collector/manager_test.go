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

package collector

import (
	"errors"
	"io"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// fakeCollector is a Collector whose Update pass is scripted by the test.
type fakeCollector struct {
	update func(ch chan<- prometheus.Metric) error
}

func (f *fakeCollector) Update(ch chan<- prometheus.Metric) error {
	return f.update(ch)
}

func testManagerLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// gaugeEmitter returns an Update func emitting one gauge with the given
// descriptor identity.
func gaugeEmitter(name, help string, labels map[string]string) func(ch chan<- prometheus.Metric) error {
	return func(ch chan<- prometheus.Metric) error {
		labelNames := slices.Sorted(maps.Keys(labels))
		labelValues := make([]string, len(labelNames))
		for i, n := range labelNames {
			labelValues[i] = labels[n]
		}
		desc := prometheus.NewDesc(name, help, labelNames, nil)
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, 1, labelValues...)
		return nil
	}
}

func newTestManager(t *testing.T, initial map[string]Collector, opts ...ManagerOption) *Manager {
	t.Helper()
	nc := &NodeCollector{Collectors: initial, logger: testManagerLogger()}
	return newManager(testManagerLogger(), nc, opts...)
}

// registerFakeFactory makes a collector available to Reload under the given
// name and removes it again when the test finishes.
func registerFakeFactory(t *testing.T, name string, c Collector) {
	t.Helper()
	if _, ok := factories[name]; ok {
		t.Fatalf("factory %q already registered", name)
	}
	factories[name] = func(*slog.Logger) (Collector, error) { return c, nil }
	t.Cleanup(func() {
		delete(factories, name)
		initiatedCollectorsMtx.Lock()
		defer initiatedCollectorsMtx.Unlock()
		delete(initiatedCollectors, name)
	})
}

// gather scrapes the manager through a throwaway registry, exactly like the
// HTTP handler does, and returns the metric families by name.
func gatherManager(t *testing.T, m *Manager) map[string]*dto.MetricFamily {
	t.Helper()
	registry := prometheus.NewRegistry()
	if err := registry.Register(m); err != nil {
		t.Fatalf("register manager: %v", err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather manager: %v", err)
	}
	out := make(map[string]*dto.MetricFamily, len(families))
	for _, family := range families {
		out[family.GetName()] = family
	}
	return out
}

func gaugeValue(t *testing.T, family *dto.MetricFamily, labels ...string) float64 {
	t.Helper()
	for _, metric := range family.GetMetric() {
		got := []string{}
		for _, label := range metric.GetLabel() {
			got = append(got, label.GetName()+"="+label.GetValue())
		}
		if slices.Equal(got, labels) {
			return metric.GetGauge().GetValue()
		}
	}
	t.Fatalf("metric with labels %v not found in family %q", labels, family.GetName())
	return 0
}

func TestManagerReloadEnableDisable(t *testing.T) {
	a := &fakeCollector{update: gaugeEmitter("node_fake_a", "fake a", nil)}
	b := &fakeCollector{update: gaugeEmitter("node_fake_b", "fake b", nil)}
	registerFakeFactory(t, "fake_a", a)
	registerFakeFactory(t, "fake_b", b)
	m := newTestManager(t, map[string]Collector{"fake_a": a})

	families := gatherManager(t, m)
	if _, ok := families["node_fake_a"]; !ok {
		t.Fatal("node_fake_a missing before reload")
	}
	if _, ok := families["node_fake_b"]; ok {
		t.Fatal("node_fake_b present before reload")
	}

	result, err := m.Reload(ReloadRequest{Enable: []string{"fake_b"}})
	if err != nil {
		t.Fatalf("enable reload failed: %v", err)
	}
	if !result.Applied || result.Generation != 2 {
		t.Fatalf("expected applied generation 2, got %+v", result)
	}
	families = gatherManager(t, m)
	for _, name := range []string{"node_fake_a", "node_fake_b"} {
		if _, ok := families[name]; !ok {
			t.Fatalf("%s missing after enabling fake_b", name)
		}
	}

	result, err = m.Reload(ReloadRequest{Disable: []string{"fake_a"}})
	if err != nil {
		t.Fatalf("disable reload failed: %v", err)
	}
	if !result.Applied || result.Generation != 3 {
		t.Fatalf("expected applied generation 3, got %+v", result)
	}
	families = gatherManager(t, m)
	if _, ok := families["node_fake_a"]; ok {
		t.Fatal("node_fake_a still published after disabling fake_a")
	}
	if _, ok := families["node_fake_b"]; !ok {
		t.Fatal("node_fake_b missing after disabling fake_a")
	}
	// The disabled collector must not even publish its scrape metrics.
	durations := families["node_scrape_collector_duration_seconds"]
	if got := gaugeValue(t, durations, "collector=fake_b"); got <= 0 {
		t.Fatal("scrape duration for fake_b missing")
	}
	for _, metric := range durations.GetMetric() {
		for _, label := range metric.GetLabel() {
			if label.GetValue() == "fake_a" {
				t.Fatal("scrape duration for disabled fake_a still published")
			}
		}
	}
	if got := gaugeValue(t, families["node_collector_active"], "collector=fake_b"); got != 1 {
		t.Fatal("node_collector_active for fake_b missing")
	}
	if status := m.Status(); !slices.Equal(status.ActiveCollectors, []string{"fake_b"}) {
		t.Fatalf("expected active collectors [fake_b], got %v", status.ActiveCollectors)
	}
}

func TestManagerReloadNoopKeepsGeneration(t *testing.T) {
	a := &fakeCollector{update: gaugeEmitter("node_fake_a", "fake a", nil)}
	registerFakeFactory(t, "fake_a", a)
	m := newTestManager(t, map[string]Collector{"fake_a": a})

	_, err := m.Reload(ReloadRequest{Enable: []string{"fake_a"}, Disable: []string{"fake_b_missing"}})
	if err == nil {
		t.Fatal("expected error for unknown collector")
	}
	result, err := m.Reload(ReloadRequest{Enable: []string{"fake_a"}})
	if err != nil {
		t.Fatalf("noop reload failed: %v", err)
	}
	if result.Applied {
		t.Fatal("noop reload must not apply a new generation")
	}
	if result.Generation != 1 {
		t.Fatalf("expected generation 1, got %d", result.Generation)
	}
	if status := m.Status(); status.Reloaded {
		t.Fatal("noop reload must not touch the observable reload state")
	}
}

func TestManagerReloadDuplicateDescriptorPolicies(t *testing.T) {
	a := &fakeCollector{update: gaugeEmitter("node_fake_dup", "help A", nil)}
	b := &fakeCollector{update: gaugeEmitter("node_fake_dup", "help B", nil)}
	registerFakeFactory(t, "fake_a", a)
	registerFakeFactory(t, "fake_b", b)
	m := newTestManager(t, map[string]Collector{"fake_a": a})

	// The default reject policy keeps the old generation when a reload would
	// redefine an existing metric name.
	_, err := m.Reload(ReloadRequest{Enable: []string{"fake_b"}, Disable: []string{"fake_a"}})
	if err == nil {
		t.Fatal("expected reject policy to refuse descriptor redefinition")
	}
	if !strings.Contains(err.Error(), "node_fake_dup") {
		t.Fatalf("error should name the conflicting metric, got: %v", err)
	}
	families := gatherManager(t, m)
	if got := families["node_fake_dup"].GetHelp(); got != "help A" {
		t.Fatalf("rejected reload changed the descriptor, help=%q", got)
	}
	status := m.Status()
	if status.Reloaded && status.LastSuccessful {
		t.Fatal("rejected reload must be observable as unsuccessful")
	}
	if status.LastError == "" {
		t.Fatal("rejected reload must record its error")
	}
	if got := gaugeValue(t, families["node_collector_last_reload_successful"]); got != 0 {
		t.Fatal("node_collector_last_reload_successful must be 0 after a rejected reload")
	}

	// The replace policy swaps the descriptor identity atomically.
	replace := DuplicatePolicyReplace
	result, err := m.Reload(ReloadRequest{Enable: []string{"fake_b"}, Disable: []string{"fake_a"}, Policy: &replace})
	if err != nil {
		t.Fatalf("replace policy reload failed: %v", err)
	}
	if !result.Applied {
		t.Fatal("replace policy reload must apply")
	}
	families = gatherManager(t, m)
	if got := families["node_fake_dup"].GetHelp(); got != "help B" {
		t.Fatalf("replace policy did not swap the descriptor, help=%q", got)
	}
}

func TestManagerReloadWithinGenerationConflict(t *testing.T) {
	a := &fakeCollector{update: gaugeEmitter("node_fake_dup", "help A", nil)}
	b := &fakeCollector{update: gaugeEmitter("node_fake_dup", "help B", nil)}
	registerFakeFactory(t, "fake_a", a)
	registerFakeFactory(t, "fake_b", b)
	m := newTestManager(t, map[string]Collector{"fake_a": a}, WithDuplicatePolicy(DuplicatePolicyReplace))

	// Two collectors describing the same metric name differently can never
	// share a generation, even under the replace policy.
	_, err := m.Reload(ReloadRequest{Enable: []string{"fake_b"}})
	if err == nil {
		t.Fatal("expected within-generation conflict to fail the reload")
	}
	if !strings.Contains(err.Error(), "fake_a") || !strings.Contains(err.Error(), "fake_b") {
		t.Fatalf("error should name both collectors, got: %v", err)
	}
	if got := m.Status().Generation; got != 1 {
		t.Fatalf("failed reload must not swap the generation, got %d", got)
	}
}

func TestManagerReloadSharedDescriptorAllowed(t *testing.T) {
	a := &fakeCollector{update: gaugeEmitter("node_fake_shared", "shared", map[string]string{"device": "a"})}
	b := &fakeCollector{update: gaugeEmitter("node_fake_shared", "shared", map[string]string{"device": "b"})}
	registerFakeFactory(t, "fake_a", a)
	registerFakeFactory(t, "fake_b", b)
	m := newTestManager(t, map[string]Collector{"fake_a": a})

	if _, err := m.Reload(ReloadRequest{Enable: []string{"fake_b"}}); err != nil {
		t.Fatalf("identical descriptors from two collectors must be allowed: %v", err)
	}
	families := gatherManager(t, m)
	if got := len(families["node_fake_shared"].GetMetric()); got != 2 {
		t.Fatalf("expected 2 series for the shared metric, got %d", got)
	}
}

func TestManagerReloadValidationErrorIsolation(t *testing.T) {
	a := &fakeCollector{update: gaugeEmitter("node_fake_a", "fake a", nil)}
	bad := &fakeCollector{update: func(chan<- prometheus.Metric) error {
		return errors.New("probe failure")
	}}
	ok := &fakeCollector{update: gaugeEmitter("node_fake_ok", "fake ok", nil)}
	registerFakeFactory(t, "fake_a", a)
	registerFakeFactory(t, "fake_bad", bad)
	registerFakeFactory(t, "fake_ok", ok)
	m := newTestManager(t, map[string]Collector{"fake_a": a})

	result, err := m.Reload(ReloadRequest{Enable: []string{"fake_bad", "fake_ok"}})
	if err != nil {
		t.Fatalf("validation failure of one collector must not fail the reload: %v", err)
	}
	if !result.Applied {
		t.Fatal("reload with an excluded collector must still apply")
	}
	if !strings.Contains(result.Excluded["fake_bad"], "probe failure") {
		t.Fatalf("expected validation error for fake_bad, got %v", result.Excluded)
	}
	families := gatherManager(t, m)
	if _, ok := families["node_fake_ok"]; !ok {
		t.Fatal("independent collector fake_ok must not be affected by fake_bad's failure")
	}
	if got := gaugeValue(t, families["node_collector_validation_error"], "collector=fake_bad"); got != 1 {
		t.Fatal("node_collector_validation_error for fake_bad missing")
	}
	if got := gaugeValue(t, families["node_collector_last_reload_successful"]); got != 0 {
		t.Fatal("reload with exclusions must not be reported as fully successful")
	}
	status := m.Status()
	if !strings.Contains(status.ValidationErrors["fake_bad"], "probe failure") {
		t.Fatalf("validation error missing from status: %v", status.ValidationErrors)
	}
}

func TestManagerReloadKeepsInFlightSnapshot(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64
	var once sync.Once
	blocking := &fakeCollector{update: func(ch chan<- prometheus.Metric) error {
		if calls.Add(1) == 1 {
			// The startup profiling probe returns immediately.
			return nil
		}
		once.Do(func() { close(started) })
		<-release
		send := gaugeEmitter("node_fake_blocking", "blocking", nil)
		return send(ch)
	}}
	registerFakeFactory(t, "fake_blocking", blocking)
	m := newTestManager(t, map[string]Collector{"fake_blocking": blocking})

	// Start a scrape that blocks inside the collector, then disable the
	// collector while the scrape is in flight.
	metricsCh := make(chan prometheus.Metric)
	done := make(chan []prometheus.Metric)
	go func() {
		var metrics []prometheus.Metric
		for metric := range metricsCh {
			metrics = append(metrics, metric)
		}
		done <- metrics
	}()
	go func() {
		m.Collect(metricsCh)
		close(metricsCh)
	}()
	<-started

	if _, err := m.Reload(ReloadRequest{Disable: []string{"fake_blocking"}}); err != nil {
		t.Fatalf("reload during in-flight scrape failed: %v", err)
	}
	close(release)

	// The in-flight scrape keeps its snapshot and still publishes the
	// disabled collector's metrics.
	found := false
	for _, metric := range <-done {
		if strings.Contains(metric.Desc().String(), "node_fake_blocking") {
			found = true
		}
	}
	if !found {
		t.Fatal("in-flight scrape lost the disabled collector's metrics")
	}

	// New scrapes use the new generation and no longer publish it.
	if _, ok := gatherManager(t, m)["node_fake_blocking"]; ok {
		t.Fatal("disabled collector still published after its scrape finished")
	}
}

func TestManagerConcurrentScrapeAndReload(t *testing.T) {
	a := &fakeCollector{update: gaugeEmitter("node_fake_flip", "help A", nil)}
	b := &fakeCollector{update: gaugeEmitter("node_fake_flip", "help B", nil)}
	registerFakeFactory(t, "fake_a", a)
	registerFakeFactory(t, "fake_b", b)
	m := newTestManager(t, map[string]Collector{"fake_a": a}, WithDuplicatePolicy(DuplicatePolicyReplace))

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var failures atomic.Int64
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				registry := prometheus.NewRegistry()
				if err := registry.Register(m); err != nil {
					failures.Add(1)
					return
				}
				// A scrape mixing two generations would surface here as a
				// descriptor consistency error.
				if _, err := registry.Gather(); err != nil {
					failures.Add(1)
					return
				}
			}
		})
	}
	for i := range 20 {
		var req ReloadRequest
		if i%2 == 0 {
			req = ReloadRequest{Enable: []string{"fake_b"}, Disable: []string{"fake_a"}}
		} else {
			req = ReloadRequest{Enable: []string{"fake_a"}, Disable: []string{"fake_b"}}
		}
		if _, err := m.Reload(req); err != nil {
			t.Fatalf("reload %d failed: %v", i, err)
		}
	}
	close(stop)
	wg.Wait()
	if got := failures.Load(); got != 0 {
		t.Fatalf("observed %d inconsistent scrapes during concurrent reloads", got)
	}
}

func TestManagerStatusMetrics(t *testing.T) {
	a := &fakeCollector{update: gaugeEmitter("node_fake_a", "fake a", nil)}
	b := &fakeCollector{update: gaugeEmitter("node_fake_b", "fake b", nil)}
	registerFakeFactory(t, "fake_a", a)
	registerFakeFactory(t, "fake_b", b)
	m := newTestManager(t, map[string]Collector{"fake_a": a})

	families := gatherManager(t, m)
	if got := gaugeValue(t, families["node_collector_generation"]); got != 1 {
		t.Fatalf("expected generation 1, got %v", got)
	}
	if _, ok := families["node_collector_last_reload_successful"]; ok {
		t.Fatal("reload metrics must not be published before the first reload")
	}

	if _, err := m.Reload(ReloadRequest{Enable: []string{"fake_b"}}); err != nil {
		t.Fatalf("reload failed: %v", err)
	}
	families = gatherManager(t, m)
	if got := gaugeValue(t, families["node_collector_generation"]); got != 2 {
		t.Fatalf("expected generation 2, got %v", got)
	}
	if got := gaugeValue(t, families["node_collector_last_reload_successful"]); got != 1 {
		t.Fatal("expected node_collector_last_reload_successful to be 1")
	}
	if got := gaugeValue(t, families["node_collector_last_reload_timestamp_seconds"]); got <= 0 {
		t.Fatal("expected a positive reload timestamp")
	}
	for _, name := range []string{"fake_a", "fake_b"} {
		if got := gaugeValue(t, families["node_collector_active"], "collector="+name); got != 1 {
			t.Fatalf("node_collector_active for %s missing", name)
		}
	}
}

func TestProfileCollectorRejectsInconsistentDescriptors(t *testing.T) {
	inconsistent := &fakeCollector{update: func(ch chan<- prometheus.Metric) error {
		ch <- prometheus.MustNewConstMetric(prometheus.NewDesc("node_fake_inconsistent", "help one", nil, nil), prometheus.GaugeValue, 1)
		ch <- prometheus.MustNewConstMetric(prometheus.NewDesc("node_fake_inconsistent", "help two", nil, nil), prometheus.GaugeValue, 2)
		return nil
	}}
	if _, err := profileCollector(inconsistent); err == nil {
		t.Fatal("expected inconsistent help texts to fail validation")
	}
}

func TestParseDuplicatePolicy(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  DuplicatePolicy
	}{
		{"reject", DuplicatePolicyReject},
		{"replace", DuplicatePolicyReplace},
	} {
		got, err := ParseDuplicatePolicy(tc.input)
		if err != nil {
			t.Fatalf("ParseDuplicatePolicy(%q) failed: %v", tc.input, err)
		}
		if got != tc.want {
			t.Fatalf("ParseDuplicatePolicy(%q) = %v, want %v", tc.input, got, tc.want)
		}
		if got.String() != tc.input {
			t.Fatalf("DuplicatePolicy.String() = %q, want %q", got, tc.input)
		}
	}
	if _, err := ParseDuplicatePolicy("bogus"); err == nil {
		t.Fatal("expected error for unknown policy")
	}
}

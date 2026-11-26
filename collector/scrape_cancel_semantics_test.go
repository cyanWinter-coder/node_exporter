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
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

var uid452TestMetricDesc = prometheus.NewDesc(
	"uid452_test_metric",
	"Test metric emitted by the successful UID 452 collector.",
	nil,
	nil,
)

var uid452LateMetricDesc = prometheus.NewDesc(
	"uid452_late_metric",
	"Test metric emitted after cancellation.",
	nil,
	nil,
)

type uid452FakeCollector struct {
	updateCalled chan struct{}
	started      chan struct{}
	finished     chan struct{}
	lateSent     chan struct{}

	release <-chan struct{}

	block          bool
	sendBeforeWait bool
	sendAfterWait  bool
	legacy         bool

	seenCtx context.Context
	seenCh  chan<- prometheus.Metric
	chans   []chan<- prometheus.Metric
}

func newUID452FakeCollector() *uid452FakeCollector {
	return &uid452FakeCollector{
		updateCalled: make(chan struct{}, 16),
		started:      make(chan struct{}, 1),
		finished:     make(chan struct{}, 16),
		lateSent:     make(chan struct{}, 16),
	}
}

// Update keeps the fake compatible with the existing Collector interface.
func (c *uid452FakeCollector) Update(ch chan<- prometheus.Metric) error {
	defer func() { c.finished <- struct{}{} }()
	c.updateCalled <- struct{}{}
	c.seenCh = ch
	c.chans = append(c.chans, ch)

	if !c.legacy {
		return errors.New("uid452: Update called on context-aware fake")
	}

	close(c.started)
	if c.sendBeforeWait {
		ch <- prometheus.MustNewConstMetric(uid452TestMetricDesc, prometheus.GaugeValue, 1)
	}
	if c.block {
		<-c.release
	}
	return nil
}

func (c *uid452FakeCollector) UpdateWithContext(ctx context.Context, ch chan<- prometheus.Metric) error {
	defer func() { c.finished <- struct{}{} }()
	c.updateCalled <- struct{}{}
	c.seenCtx = ctx
	c.seenCh = ch
	c.chans = append(c.chans, ch)
	close(c.started)

	if c.sendBeforeWait {
		ch <- prometheus.MustNewConstMetric(uid452TestMetricDesc, prometheus.GaugeValue, 1)
	}

	if c.block {
		<-ctx.Done()
	}

	if c.sendAfterWait {
		m := prometheus.MustNewConstMetric(uid452LateMetricDesc, prometheus.GaugeValue, 2)
		select {
		case ch <- m:
			c.lateSent <- struct{}{}
		case <-time.After(2 * time.Second):
			return fmt.Errorf("uid452: timed out sending late metric")
		}
	}

	if c.block {
		return ctx.Err()
	}
	return nil
}

func uid452Collect(ctx context.Context, t *testing.T, nc NodeCollector) []prometheus.Metric {
	t.Helper()

	ch := make(chan prometheus.Metric, 256)
	done := make(chan struct{})
	go func() {
		nc.CollectWithContext(ctx, ch)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("CollectWithContext did not return after cancellation")
	}

	close(ch)
	var metrics []prometheus.Metric
	for m := range ch {
		metrics = append(metrics, m)
	}
	return metrics
}

func uid452StatusMetrics(t *testing.T, metrics []prometheus.Metric) map[string]float64 {
	t.Helper()
	status := make(map[string]float64)
	for _, m := range metrics {
		if m.Desc() != scrapeSuccessDesc {
			continue
		}
		pb := &dto.Metric{}
		if err := m.Write(pb); err != nil {
			t.Fatalf("write status metric: %v", err)
		}
		var collector string
		for _, lp := range pb.Label {
			if lp.GetName() == "collector" {
				collector = lp.GetValue()
			}
		}
		status[collector] = pb.GetGauge().GetValue()
	}
	return status
}

func uid452HasDesc(metrics []prometheus.Metric, desc *prometheus.Desc) bool {
	for _, m := range metrics {
		if m.Desc() == desc {
			return true
		}
	}
	return false
}

func TestUID452CancelPropagatesAndPublishesPartialResults(t *testing.T) {
	ok := newUID452FakeCollector()
	ok.sendBeforeWait = true

	blocked := newUID452FakeCollector()
	blocked.block = true

	nc := NodeCollector{Collectors: map[string]Collector{
		"uid452_ok":      ok,
		"uid452_blocked": blocked,
	}}

	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan prometheus.Metric, 256)
	metricsCh := make(chan []prometheus.Metric, 1)
	okMetric := make(chan struct{}, 1)

	go func() {
		var metrics []prometheus.Metric
		for m := range ch {
			metrics = append(metrics, m)
			if m.Desc() == uid452TestMetricDesc {
				select {
				case okMetric <- struct{}{}:
				default:
				}
			}
		}
		metricsCh <- metrics
	}()

	done := make(chan struct{})
	go func() {
		nc.CollectWithContext(ctx, ch)
		close(done)
	}()

	select {
	case <-okMetric:
	case <-time.After(5 * time.Second):
		t.Fatal("metric from completed collector was not published before cancellation")
	}

	select {
	case <-blocked.started:
	case <-time.After(5 * time.Second):
		t.Fatal("blocking collector did not start")
	}

	if blocked.seenCtx == nil {
		t.Fatal("context was not passed to context-aware collector")
	}

	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for scrape result")
	}
	close(ch)

	var metrics []prometheus.Metric
	select {
	case metrics = <-metricsCh:
	case <-time.After(5 * time.Second):
		t.Fatal("metric reader did not finish")
	}

	status := uid452StatusMetrics(t, metrics)
	if want, got := 1.0, status["uid452_ok"]; want != got {
		t.Fatalf("successful collector status = %v, want %v (all statuses: %v)", got, want, status)
	}
	if want, got := 0.0, status["uid452_blocked"]; want != got {
		t.Fatalf("blocked collector status = %v, want %v (all statuses: %v)", got, want, status)
	}
	if !uid452HasDesc(metrics, uid452TestMetricDesc) {
		t.Fatal("metric from completed collector was not published")
	}

	select {
	case <-blocked.seenCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("collector context was not canceled")
	}
}

func TestUID452CanceledContextStartsNoCollectors(t *testing.T) {
	c := newUID452FakeCollector()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	metrics := uid452Collect(ctx, t, NodeCollector{Collectors: map[string]Collector{"uid452_canceled": c}})

	select {
	case <-c.started:
		t.Fatal("collector started after context was already canceled")
	default:
	}

	status := uid452StatusMetrics(t, metrics)
	if got := status["uid452_canceled"]; got != 0 {
		t.Fatalf("canceled collector status = %v, want 0", got)
	}
}

func TestUID452LateSendsAreNotPublished(t *testing.T) {
	blocked := newUID452FakeCollector()
	blocked.block = true
	blocked.sendAfterWait = true

	nc := NodeCollector{Collectors: map[string]Collector{"uid452_late": blocked}}
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan []prometheus.Metric, 1)
	go func() {
		ch := make(chan prometheus.Metric, 256)
		nc.CollectWithContext(ctx, ch)
		close(ch)
		var metrics []prometheus.Metric
		for m := range ch {
			metrics = append(metrics, m)
		}
		done <- metrics
	}()

	<-blocked.started
	cancel()

	select {
	case <-blocked.lateSent:
	case <-time.After(5 * time.Second):
		t.Fatal("late send was not drained/released after cancellation")
	}

	select {
	case metrics := <-done:
		if uid452HasDesc(metrics, uid452LateMetricDesc) {
			t.Fatal("metric sent after cancellation was published")
		}
		if got := uid452StatusMetrics(t, metrics)["uid452_late"]; got != 0 {
			t.Fatalf("late collector status = %v, want 0", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("scrape did not finish")
	}
}

func TestUID452QueuedCollectorNotStartedAfterCancel(t *testing.T) {
	collectors := map[string]Collector{}
	blockers := make([]*uid452FakeCollector, maxConcurrentCollectors)
	for i := range blockers {
		c := newUID452FakeCollector()
		c.block = true
		blockers[i] = c
		collectors[fmt.Sprintf("uid452_queue_%02d", i)] = c
	}
	late := newUID452FakeCollector()
	collectors["uid452_queue_zzz_late"] = late

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan []prometheus.Metric, 1)
	go func() {
		result <- uid452Collect(ctx, t, NodeCollector{Collectors: collectors})
	}()

	for i, c := range blockers {
		select {
		case <-c.started:
		case <-time.After(5 * time.Second):
			t.Fatalf("blocking collector %d did not start", i)
		}
	}

	cancel()
	select {
	case <-result:
	case <-time.After(5 * time.Second):
		t.Fatal("scrape did not return after cancellation")
	}

	select {
	case <-late.started:
		t.Fatal("queued collector started after cancellation")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestUID452RepeatedScrapesUseDistinctChannels(t *testing.T) {
	c := newUID452FakeCollector()
	c.block = true
	c.sendAfterWait = true
	nc := NodeCollector{Collectors: map[string]Collector{"uid452_repeat": c}}

	ctx1, cancel1 := context.WithCancel(context.Background())
	firstDone := make(chan struct{})
	go func() {
		ch1 := make(chan prometheus.Metric, 16)
		nc.CollectWithContext(ctx1, ch1)
		close(firstDone)
	}()
	<-c.started
	cancel1()
	<-c.lateSent
	<-c.finished
	<-firstDone

	// Reset the one-shot signals for the second scrape.
	c.started = make(chan struct{}, 1)
	c.lateSent = make(chan struct{}, 1)
	c.block = false
	c.sendAfterWait = false

	metrics := uid452Collect(t.Context(), t, nc)

	if len(c.chans) < 2 {
		t.Fatalf("collector saw %d scrape channels, want at least 2", len(c.chans))
	}
	if c.chans[0] == c.chans[1] {
		t.Fatal("repeated scrape reused the previous metric channel")
	}
	if uid452HasDesc(metrics, uid452LateMetricDesc) {
		t.Fatal("late metric from previous scrape contaminated the next scrape")
	}
	if got := uid452StatusMetrics(t, metrics)["uid452_repeat"]; got != 1 {
		t.Fatalf("second scrape status = %v, want 1", got)
	}
}

func TestUID452LegacyCollectorRemainsCompatible(t *testing.T) {
	c := newUID452FakeCollector()
	c.legacy = true
	c.sendBeforeWait = true

	metrics := uid452Collect(context.Background(), t, NodeCollector{Collectors: map[string]Collector{"uid452_legacy": c}})

	select {
	case <-c.updateCalled:
	default:
		t.Fatal("legacy Update was not called")
	}
	if !uid452HasDesc(metrics, uid452TestMetricDesc) {
		t.Fatal("legacy collector metric was not published")
	}
	if got := uid452StatusMetrics(t, metrics)["uid452_legacy"]; got != 1 {
		t.Fatalf("legacy collector status = %v, want 1", got)
	}
}

func TestUID452LegacyBlockingCollectorCancellationReturns(t *testing.T) {
	release := make(chan struct{})
	c := newUID452FakeCollector()
	c.legacy = true
	c.block = true
	c.release = release
	defer close(release)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan []prometheus.Metric, 1)
	go func() {
		result <- uid452Collect(ctx, t, NodeCollector{Collectors: map[string]Collector{"uid452_legacy_blocked": c}})
	}()

	<-c.started
	cancel()

	select {
	case metrics := <-result:
		if got := uid452StatusMetrics(t, metrics)["uid452_legacy_blocked"]; got != 0 {
			t.Fatalf("blocked legacy collector status = %v, want 0", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CollectWithContext waited for a legacy collector that ignored cancellation")
	}
}

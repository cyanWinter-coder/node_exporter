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

// Package collector includes all individual collectors to gather and export system metrics.
package collector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/client_golang/prometheus"
)

// Namespace defines the common namespace to be used by all metrics.
const namespace = "node"

var (
	scrapeDurationDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "scrape", "collector_duration_seconds"),
		"node_exporter: Duration of a collector scrape.",
		[]string{"collector"},
		nil,
	)
	scrapeSuccessDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "scrape", "collector_success"),
		"node_exporter: Whether a collector succeeded.",
		[]string{"collector"},
		nil,
	)
)

const (
	defaultEnabled  = true
	defaultDisabled = false
)

var (
	factories              = make(map[string]func(logger *slog.Logger) (Collector, error))
	initiatedCollectorsMtx = sync.Mutex{}
	initiatedCollectors    = make(map[string]Collector)
	collectorState         = make(map[string]*bool)
	forcedCollectors       = map[string]bool{} // collectors which have been explicitly enabled or disabled
)

func registerCollector(collector string, isDefaultEnabled bool, factory func(logger *slog.Logger) (Collector, error)) {
	var helpDefaultState string
	if isDefaultEnabled {
		helpDefaultState = "enabled"
	} else {
		helpDefaultState = "disabled"
	}

	flagName := fmt.Sprintf("collector.%s", collector)
	flagHelp := fmt.Sprintf("Enable the %s collector (default: %s).", collector, helpDefaultState)
	defaultValue := fmt.Sprintf("%v", isDefaultEnabled)

	flag := kingpin.Flag(flagName, flagHelp).Default(defaultValue).Action(collectorFlagAction(collector)).Bool()
	collectorState[collector] = flag

	factories[collector] = factory
}

// NodeCollector implements the prometheus.Collector interface.
type NodeCollector struct {
	Collectors map[string]Collector
	logger     *slog.Logger
}

// DisableDefaultCollectors sets the collector state to false for all collectors which
// have not been explicitly enabled on the command line.
func DisableDefaultCollectors() {
	for c := range collectorState {
		if _, ok := forcedCollectors[c]; !ok {
			*collectorState[c] = false
		}
	}
}

// collectorFlagAction generates a new action function for the given collector
// to track whether it has been explicitly enabled or disabled from the command line.
// A new action function is needed for each collector flag because the ParseContext
// does not contain information about which flag called the action.
// See: https://github.com/alecthomas/kingpin/issues/294
func collectorFlagAction(collector string) func(ctx *kingpin.ParseContext) error {
	return func(_ *kingpin.ParseContext) error {
		forcedCollectors[collector] = true
		return nil
	}
}

// NewNodeCollector creates a new NodeCollector.
func NewNodeCollector(logger *slog.Logger, filters ...string) (*NodeCollector, error) {
	f := make(map[string]bool)
	for _, filter := range filters {
		enabled, exist := collectorState[filter]
		if !exist {
			return nil, fmt.Errorf("missing collector: %s", filter)
		}
		if !*enabled {
			return nil, fmt.Errorf("disabled collector: %s", filter)
		}
		f[filter] = true
	}
	collectors := make(map[string]Collector)
	initiatedCollectorsMtx.Lock()
	defer initiatedCollectorsMtx.Unlock()
	for key, enabled := range collectorState {
		if !*enabled || (len(f) > 0 && !f[key]) {
			continue
		}
		if collector, ok := initiatedCollectors[key]; ok {
			collectors[key] = collector
		} else {
			collector, err := factories[key](logger.With("collector", key))
			if err != nil {
				return nil, err
			}
			collectors[key] = collector
			initiatedCollectors[key] = collector
		}
	}
	return &NodeCollector{Collectors: collectors, logger: logger}, nil
}

// NewNodeCollectorWithCollectors builds a NodeCollector from the given
// collector instances. It is intended for tests which exercise the collection
// machinery without going through collector registration.
func NewNodeCollectorWithCollectors(logger *slog.Logger, collectors map[string]Collector) *NodeCollector {
	return &NodeCollector{Collectors: collectors, logger: logger}
}

// Describe implements the prometheus.Collector interface.
func (n NodeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- scrapeDurationDesc
	ch <- scrapeSuccessDesc
}

// collectorMetricChanCap mirrors the buffering prometheus.Registry uses for
// the channel handed to Collect. Keeping the same size preserves the existing
// backpressure behavior when a scrape is not canceled.
const collectorMetricChanCap = 1000

// maxConcurrentCollectors bounds how many collectors run at the same time;
// any further collectors are queued and are not started once the scrape
// context is canceled.
const maxConcurrentCollectors = 16

// Collect implements the prometheus.Collector interface.
func (n NodeCollector) Collect(ch chan<- prometheus.Metric) {
	n.CollectWithContext(context.Background(), ch)
}

// CollectWithContext runs all collectors under the given context. The context
// is passed to every collector implementing ContextCollector; collectors that
// only implement Collector run as before. Every invocation creates its own
// internal metric and result channels, so concurrent scrapes never share a
// channel or buffer.
//
// On context cancellation, queued collectors are skipped, and the running
// collectors are left to unwind on their own. Metrics emitted before the
// cancellation are still published, while any metric emitted afterwards is
// dropped. Every collector which did not complete successfully gets a
// node_scrape_collector_success metric of 0, so partial results are published
// together with an explicit failure signal.
func (n NodeCollector) CollectWithContext(ctx context.Context, ch chan<- prometheus.Metric) {
	if ctx == nil {
		ctx = context.Background()
	}
	if n.logger == nil {
		n.logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	metricCh := make(chan prometheus.Metric, collectorMetricChanCap)
	// resultCh is buffered for one entry per collector so collector goroutines
	// never block on it, even when the scrape has been canceled.
	resultCh := make(chan collectorResult, len(n.Collectors))

	// A bounded worker pool gives the collectors a real queue: at most
	// maxConcurrentCollectors run concurrently, and queued jobs are skipped
	// instead of started after cancellation. Job names are enqueued in sorted
	// order so scheduling is deterministic across scrapes.
	names := make([]string, 0, len(n.Collectors))
	for name := range n.Collectors {
		names = append(names, name)
	}
	sort.Strings(names)
	jobs := make(chan string, len(n.Collectors))
	for _, name := range names {
		jobs <- name
	}
	close(jobs)

	var wg sync.WaitGroup
	workers := min(maxConcurrentCollectors, len(n.Collectors))
	for range workers {
		wg.Go(func() {
			for name := range jobs {
				// Do not start a queued collector once the scrape is canceled.
				select {
				case <-ctx.Done():
					resultCh <- collectorResult{name: name, err: ctx.Err()}
					continue
				default:
				}
				begin := time.Now()
				err := updateCollector(ctx, n.Collectors[name], metricCh)
				resultCh <- collectorResult{
					name:     name,
					err:      err,
					duration: time.Since(begin),
				}
			}
		})
	}

	// The forwarder owns the decision of what reaches the output channel.
	//
	// Before stopCh closes, every metric read from metricCh is forwarded to the
	// output channel, so it stays part of the (possibly partial) result, and
	// backpressure on ch is preserved. Once stopCh closes, the metrics already
	// read are flushed once in a best-effort, non-blocking pass, and everything
	// read afterwards is discarded while still being drained, so late
	// collectors never block on a full internal buffer.
	//
	// sendsDone is closed after the last possible send to ch. After it closes,
	// nothing writes to ch anymore, even though draining may continue for a
	// stuck context-unaware collector.
	stopCh := make(chan struct{})
	forwarderDone := make(chan struct{})
	sendsDone := make(chan struct{})
	go func() {
		defer close(forwarderDone)
		closeSends := sync.OnceFunc(func() { close(sendsDone) })

		var queue []prometheus.Metric
		discarding := false

		for {
			// Read phase: pull one metric (or learn the channel closed).
			var metric prometheus.Metric
			var ok bool
			if discarding {
				// After cancellation: keep draining without queuing or sending,
				// so late collectors never block on a full internal buffer.
				metric, ok = <-metricCh
			} else {
				select {
				case <-stopCh:
					discarding = true
					// Flush what was accepted before the cancellation without
					// blocking, then stop sending.
					for len(queue) > 0 {
						select {
						case ch <- queue[0]:
							queue = queue[1:]
						default:
							queue = nil
						}
					}
					closeSends()
					continue
				case metric, ok = <-metricCh:
				}
			}

			// Cancellation may already have happened while select chose a
			// metric that was buffered. Treat that metric as late and discard
			// it instead of forwarding it.
			if !discarding && ctx.Err() != nil {
				discarding = true
				closeSends()
			}

			if discarding {
				if !ok {
					closeSends()
					return
				}
				// Drop late metrics; the read alone keeps collectors unblocked.
				continue
			}

			if !ok {
				// Clean shutdown: deliver everything accepted, blocking as
				// needed to preserve the original backpressure behavior.
				for _, m := range queue {
					ch <- m
				}
				closeSends()
				return
			}

			queue = append(queue, metric)

			// Send phase: drain the queue before reading the next metric. This
			// is a plain blocking send; cancellation is picked up on the next
			// outer iteration, which is sufficient because the registry keeps
			// draining ch until Collect returns.
			for len(queue) > 0 {
				ch <- queue[0]
				queue = queue[1:]
			}
		}
	}()
	// Close the internal channel once every collector worker has returned. The
	// forwarder is the sole remaining reader and drains it (delivering in the
	// normal phase, discarding after cancellation), so there must not be a
	// second drain here: two readers on metricCh would swallow metrics.
	go func() {
		wg.Wait()
		close(metricCh)
	}()

	pending := make(map[string]struct{}, len(n.Collectors))
	for name := range n.Collectors {
		pending[name] = struct{}{}
	}

	for len(pending) > 0 {
		select {
		case <-ctx.Done():
			// Stop forwarding metrics emitted after the cancellation. Metrics
			// the forwarder already accepted stay part of the partial result.
			close(stopCh)

			// Drain results already in flight so completed collectors are
			// still reported with their real outcome.
			for {
				select {
				case result := <-resultCh:
					delete(pending, result.name)
					n.emitStatus(ch, result)
					continue
				default:
				}
				break
			}
			// Everything still pending did not complete the scrape and is
			// reported as failed explicitly.
			for name := range pending {
				n.emitStatus(ch, collectorResult{name: name, err: ctx.Err()})
			}
			// Wait until the forwarder has performed its last send. After
			// this point nothing writes to ch anymore, even though the
			// forwarder may keep draining for a stuck legacy collector.
			<-sendsDone
			return
		case result := <-resultCh:
			delete(pending, result.name)
			n.emitStatus(ch, result)
		}
	}

	// Clean shutdown: all collectors are done and the internal channel is
	// closing; wait until every accepted metric has reached the output channel.
	<-forwarderDone
}

// CollectContext is an alias for CollectWithContext.
func (n NodeCollector) CollectContext(ctx context.Context, ch chan<- prometheus.Metric) {
	n.CollectWithContext(ctx, ch)
}

type collectorResult struct {
	name     string
	err      error
	duration time.Duration
}

// emitStatus sends the per-collector duration and success metrics, including
// for collectors which did not finish a canceled scrape. The registry keeps
// draining the output channel until Collect returns, so these are sent
// unconditionally rather than gated on the canceled context.
func (n NodeCollector) emitStatus(ch chan<- prometheus.Metric, result collectorResult) {
	var success float64
	if result.err != nil {
		switch {
		case IsNoDataError(result.err):
			n.logger.Debug("collector returned no data", "name", result.name, "duration_seconds", result.duration.Seconds(), "err", result.err)
		case errors.Is(result.err, context.Canceled), errors.Is(result.err, context.DeadlineExceeded):
			n.logger.Debug("collector canceled", "name", result.name, "duration_seconds", result.duration.Seconds(), "err", result.err)
		default:
			n.logger.Error("collector failed", "name", result.name, "duration_seconds", result.duration.Seconds(), "err", result.err)
		}
	} else {
		n.logger.Debug("collector succeeded", "name", result.name, "duration_seconds", result.duration.Seconds())
		success = 1
	}
	ch <- prometheus.MustNewConstMetric(scrapeDurationDesc, prometheus.GaugeValue, result.duration.Seconds(), result.name)
	ch <- prometheus.MustNewConstMetric(scrapeSuccessDesc, prometheus.GaugeValue, success, result.name)
}

// updateCollector dispatches to the supported context-aware method when the
// collector provides one and falls back to Update otherwise, keeping the
// existing Collector interface fully compatible.
func updateCollector(ctx context.Context, c Collector, ch chan<- prometheus.Metric) error {
	switch cc := c.(type) {
	case ContextCollector:
		return cc.UpdateContext(ctx, ch)
	case CollectorWithContext:
		return cc.UpdateWithContext(ctx, ch)
	default:
		return c.Update(ch)
	}
}

// execute retains the pre-cancellation helper for compatibility with focused
// collector tests and external callers that may have relied on the package
// internal entry point.
//
//nolint:unused // Kept as a stable package-internal compatibility helper.
func execute(name string, c Collector, ch chan<- prometheus.Metric, logger *slog.Logger) {
	begin := time.Now()
	err := c.Update(ch)
	duration := time.Since(begin)
	var success float64

	if err != nil {
		if IsNoDataError(err) {
			logger.Debug("collector returned no data", "name", name, "duration_seconds", duration.Seconds(), "err", err)
		} else {
			logger.Error("collector failed", "name", name, "duration_seconds", duration.Seconds(), "err", err)
		}
		success = 0
	} else {
		logger.Debug("collector succeeded", "name", name, "duration_seconds", duration.Seconds())
		success = 1
	}
	ch <- prometheus.MustNewConstMetric(scrapeDurationDesc, prometheus.GaugeValue, duration.Seconds(), name)
	ch <- prometheus.MustNewConstMetric(scrapeSuccessDesc, prometheus.GaugeValue, success, name)
}

// Collector is the interface a collector has to implement.
type Collector interface {
	// Get new metrics and expose them via prometheus registry.
	Update(ch chan<- prometheus.Metric) error
}

// ContextCollector is an optional extension of Collector. Collectors
// implementing it receive the scrape context and should stop their work and
// return once the context is canceled. Collectors only implementing Collector
// keep working unchanged and are treated as blocking collectors on
// cancellation.
type ContextCollector interface {
	Collector
	// UpdateContext behaves like Update but honors the given context.
	UpdateContext(ctx context.Context, ch chan<- prometheus.Metric) error
}

// CollectorWithContext is the alternate context-aware spelling. NodeCollector
// accepts either UpdateContext or UpdateWithContext through structural type
// assertions.
//
//nolint:revive // The explicit Collector prefix is the compatibility API name.
type CollectorWithContext interface {
	Collector
	UpdateWithContext(ctx context.Context, ch chan<- prometheus.Metric) error
}

type typedDesc struct {
	desc      *prometheus.Desc
	valueType prometheus.ValueType
}

func (d *typedDesc) mustNewConstMetric(value float64, labels ...string) prometheus.Metric {
	return prometheus.MustNewConstMetric(d.desc, d.valueType, value, labels...)
}

// ErrNoData indicates the collector found no data to collect, but had no other error.
var ErrNoData = errors.New("collector returned no data")

func IsNoDataError(err error) bool {
	return err == ErrNoData
}

// pushMetric helps construct and convert a variety of value types into Prometheus float64 metrics.
func pushMetric(ch chan<- prometheus.Metric, fieldDesc *prometheus.Desc, value any, valueType prometheus.ValueType, labelValues ...string) {
	var fVal float64
	switch val := value.(type) {
	case uint8:
		fVal = float64(val)
	case uint16:
		fVal = float64(val)
	case uint32:
		fVal = float64(val)
	case uint64:
		fVal = float64(val)
	case int64:
		fVal = float64(val)
	case *uint8:
		if val == nil {
			return
		}
		fVal = float64(*val)
	case *uint16:
		if val == nil {
			return
		}
		fVal = float64(*val)
	case *uint32:
		if val == nil {
			return
		}
		fVal = float64(*val)
	case *uint64:
		if val == nil {
			return
		}
		fVal = float64(*val)
	case *int64:
		if val == nil {
			return
		}
		fVal = float64(*val)
	default:
		return
	}

	ch <- prometheus.MustNewConstMetric(fieldDesc, valueType, fVal, labelValues...)
}

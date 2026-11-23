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
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	collectorGenerationDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "collector", "generation"),
		"node_exporter: Current collector configuration generation, incremented by every applied reload.",
		nil, nil,
	)
	collectorLastReloadSuccessDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "collector", "last_reload_successful"),
		"node_exporter: Whether the last collector reload was applied without validation errors.",
		nil, nil,
	)
	collectorLastReloadTimestampDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "collector", "last_reload_timestamp_seconds"),
		"node_exporter: Unix timestamp of the last collector reload attempt.",
		nil, nil,
	)
	collectorValidationErrorDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "collector", "validation_error"),
		"node_exporter: Whether the collector was excluded by descriptor validation during the last reload.",
		[]string{"collector"}, nil,
	)
	collectorActiveDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "collector", "active"),
		"node_exporter: Whether the collector is part of the active scrape generation.",
		[]string{"collector"}, nil,
	)
)

// DuplicatePolicy controls how a reload handles metric names that the
// requested collector generation describes differently than the active
// generation.
type DuplicatePolicy int

const (
	// DuplicatePolicyReject rejects a reload that would redefine the
	// descriptor identity of an already exported metric name. It is the
	// default policy.
	DuplicatePolicyReject DuplicatePolicy = iota
	// DuplicatePolicyReplace lets a reload atomically replace the descriptor
	// identity of an already exported metric name.
	DuplicatePolicyReplace
)

// String returns the policy name used by configuration and the reload API.
func (p DuplicatePolicy) String() string {
	if p == DuplicatePolicyReplace {
		return "replace"
	}
	return "reject"
}

// ParseDuplicatePolicy parses a policy name, "reject" or "replace".
func ParseDuplicatePolicy(s string) (DuplicatePolicy, error) {
	switch s {
	case "reject":
		return DuplicatePolicyReject, nil
	case "replace":
		return DuplicatePolicyReplace, nil
	default:
		return DuplicatePolicyReject, fmt.Errorf("unknown duplicate policy %q, want \"reject\" or \"replace\"", s)
	}
}

// descriptorIdentity captures everything Prometheus uses to decide whether
// two descriptors with the same metric name describe the same metric.
type descriptorIdentity struct {
	help       string
	typ        string
	labelNames []string
}

func (d descriptorIdentity) equal(o descriptorIdentity) bool {
	return d.help == o.help && d.typ == o.typ && slices.Equal(d.labelNames, o.labelNames)
}

func (d descriptorIdentity) String() string {
	return fmt.Sprintf("help=%q type=%s labels=%v", d.help, d.typ, d.labelNames)
}

// collectorProfile is the validated descriptor set of one collector.
type collectorProfile struct {
	// descriptors maps a metric name to its descriptor identity.
	descriptors map[string]descriptorIdentity
	// validated is false when the profile could not be completed, e.g. the
	// collector had no data when it was profiled at startup. Unvalidated
	// profiles take part in no consistency checks.
	validated bool
}

// generation is an immutable snapshot of the collectors taking part in a
// scrape. Reload swaps generations atomically, so a scrape never mixes two
// generations and an in-flight scrape keeps serving the generation it
// started with.
type generation struct {
	id         uint64
	collectors *NodeCollector
	profiles   map[string]collectorProfile
	names      []string
}

// managerState is the observable reload state, swapped atomically together
// with the generation.
type managerState struct {
	generation       uint64
	reloaded         bool
	lastReload       time.Time
	lastSuccess      bool
	lastError        string
	validationErrors map[string]string
	activeCollectors []string
}

// Manager owns the active collector generation and applies runtime reloads.
// It implements prometheus.Collector and can be registered anywhere a
// NodeCollector was registered before.
type Manager struct {
	logger *slog.Logger
	policy DuplicatePolicy

	reloadMtx sync.Mutex
	genID     uint64
	current   atomic.Pointer[generation]
	state     atomic.Pointer[managerState]
}

// ManagerOption configures a Manager.
type ManagerOption func(*Manager)

// WithDuplicatePolicy sets the default policy for descriptor identity
// conflicts between the active and a reloaded generation.
func WithDuplicatePolicy(policy DuplicatePolicy) ManagerOption {
	return func(m *Manager) { m.policy = policy }
}

// NewManager creates a Manager holding the collectors enabled via command
// line flags, like NewNodeCollector.
func NewManager(logger *slog.Logger, opts ...ManagerOption) (*Manager, error) {
	nc, err := NewNodeCollector(logger)
	if err != nil {
		return nil, err
	}
	return newManager(logger, nc, opts...), nil
}

// newManager builds a Manager around an already assembled NodeCollector. The
// initial collectors are profiled on a best-effort basis: a collector that
// cannot be validated yet stays active (startup compatibility) but takes
// part in no descriptor consistency checks until it is re-enabled through
// Reload.
func newManager(logger *slog.Logger, nc *NodeCollector, opts ...ManagerOption) *Manager {
	m := &Manager{logger: logger, genID: 1}
	for _, opt := range opts {
		opt(m)
	}
	profiles := make(map[string]collectorProfile, len(nc.Collectors))
	names := make([]string, 0, len(nc.Collectors))
	for name, c := range nc.Collectors {
		descriptors, err := profileCollector(c)
		if err != nil {
			logger.Debug("could not validate collector descriptors at startup", "collector", name, "err", err)
			profiles[name] = collectorProfile{}
		} else {
			profiles[name] = collectorProfile{descriptors: descriptors, validated: true}
		}
		names = append(names, name)
	}
	slices.Sort(names)
	if _, conflicts := mergeIdentities(profiles, names); len(conflicts) > 0 {
		logger.Warn("enabled collectors describe the same metric name differently", "conflicts", strings.Join(conflicts, "; "))
	}
	m.current.Store(&generation{id: m.genID, collectors: nc, profiles: profiles, names: names})
	m.state.Store(&managerState{generation: m.genID, activeCollectors: names})
	return m
}

// ReloadRequest describes a runtime change of the active collector set.
type ReloadRequest struct {
	// Enable lists collectors to add to the active generation.
	Enable []string `json:"enable"`
	// Disable lists collectors to remove from the active generation.
	Disable []string `json:"disable"`
	// Policy overrides the manager's duplicate descriptor policy for this
	// reload. Nil uses the manager default.
	Policy *DuplicatePolicy `json:"policy,omitempty"`
}

// ReloadResult reports the outcome of a Reload call.
type ReloadResult struct {
	// Applied is true when a new generation replaced the active one.
	Applied bool `json:"applied"`
	// Generation is the id of the active generation after the call.
	Generation uint64 `json:"generation"`
	// Active lists the collectors of the active generation.
	Active []string `json:"active"`
	// Excluded maps collectors that failed descriptor validation to the
	// validation error. Excluded collectors stay inactive while the rest of
	// the requested change is applied.
	Excluded map[string]string `json:"excluded,omitempty"`
}

// ReloadStatus is the observable state of the collector manager.
type ReloadStatus struct {
	Generation       uint64            `json:"generation"`
	Reloaded         bool              `json:"reloaded"`
	LastReload       time.Time         `json:"last_reload,omitzero"`
	LastSuccessful   bool              `json:"last_successful"`
	LastError        string            `json:"last_error,omitempty"`
	ValidationErrors map[string]string `json:"validation_errors,omitempty"`
	ActiveCollectors []string          `json:"active_collectors"`
}

// Status returns a snapshot of the manager's observable reload state.
func (m *Manager) Status() ReloadStatus {
	st := m.state.Load()
	return ReloadStatus{
		Generation:       st.generation,
		Reloaded:         st.reloaded,
		LastReload:       st.lastReload,
		LastSuccessful:   st.lastSuccess,
		LastError:        st.lastError,
		ValidationErrors: maps.Clone(st.validationErrors),
		ActiveCollectors: slices.Clone(st.activeCollectors),
	}
}

// Collectors returns the names of the collectors in the active generation.
func (m *Manager) Collectors() []string {
	return slices.Clone(m.state.Load().activeCollectors)
}

// Filtered returns a collector scraping only the named collectors of the
// generation that is active when the call is made. With no filters it
// returns the whole active generation.
func (m *Manager) Filtered(filters ...string) (*NodeCollector, error) {
	gen := m.current.Load()
	if len(filters) == 0 {
		return gen.collectors, nil
	}
	collectors := make(map[string]Collector, len(filters))
	for _, name := range filters {
		c, ok := gen.collectors.Collectors[name]
		if !ok {
			return nil, fmt.Errorf("collector %q is not part of the active generation", name)
		}
		collectors[name] = c
	}
	return &NodeCollector{Collectors: collectors, logger: m.logger}, nil
}

// Reload validates the requested collector set and, on success, atomically
// swaps it in as the active generation. Collectors that fail descriptor
// validation are excluded from the new generation and reported in the
// result; independent collectors are still applied. A reload that would
// leave the generation internally inconsistent, or redefine an existing
// descriptor under the reject policy, fails without changing the active
// generation.
func (m *Manager) Reload(req ReloadRequest) (*ReloadResult, error) {
	m.reloadMtx.Lock()
	defer m.reloadMtx.Unlock()

	cur := m.current.Load()

	names := make(map[string]struct{}, len(cur.names))
	for _, name := range cur.names {
		names[name] = struct{}{}
	}
	for _, name := range req.Disable {
		if _, ok := factories[name]; !ok {
			return nil, fmt.Errorf("unknown collector: %s", name)
		}
		delete(names, name)
	}
	for _, name := range req.Enable {
		if _, ok := factories[name]; !ok {
			return nil, fmt.Errorf("unknown collector: %s", name)
		}
		names[name] = struct{}{}
	}
	target := slices.Sorted(maps.Keys(names))
	if slices.Equal(target, cur.names) {
		return &ReloadResult{Generation: cur.id, Active: slices.Clone(cur.names)}, nil
	}

	policy := m.policy
	if req.Policy != nil {
		policy = *req.Policy
	}

	// Carried-over collectors keep their instance and their validated
	// profile. Newly enabled collectors must complete descriptor validation
	// before they may join the generation.
	collectors := make(map[string]Collector, len(target))
	profiles := make(map[string]collectorProfile, len(target))
	excluded := map[string]string{}
	for _, name := range target {
		if c, ok := cur.collectors.Collectors[name]; ok {
			collectors[name] = c
			profiles[name] = cur.profiles[name]
			continue
		}
		c, err := instantiateCollector(name, m.logger)
		if err != nil {
			excluded[name] = err.Error()
			continue
		}
		descriptors, err := profileCollector(c)
		if err != nil {
			excluded[name] = err.Error()
			continue
		}
		collectors[name] = c
		profiles[name] = collectorProfile{descriptors: descriptors, validated: true}
	}

	reject := func(err error) (*ReloadResult, error) {
		m.logger.Warn("collector reload rejected", "err", err)
		m.state.Store(&managerState{
			generation:       cur.id,
			reloaded:         true,
			lastReload:       time.Now(),
			lastSuccess:      false,
			lastError:        err.Error(),
			validationErrors: excluded,
			activeCollectors: slices.Clone(cur.names),
		})
		return nil, err
	}

	// Two collectors in the same generation must never describe the same
	// metric name differently; such a generation can never be scraped
	// consistently, no matter the duplicate policy.
	newIdentities, conflicts := mergeIdentities(profiles, target)
	if len(conflicts) > 0 {
		return reject(fmt.Errorf("inconsistent descriptors in requested collectors: %s", strings.Join(conflicts, "; ")))
	}

	// A metric name that stays exported must not silently change its
	// descriptor identity between generations unless replacement is
	// configured.
	oldIdentities, _ := mergeIdentities(cur.profiles, cur.names)
	var redefined []string
	for metric, identity := range newIdentities {
		if prev, ok := oldIdentities[metric]; ok && !prev.equal(identity) {
			redefined = append(redefined, fmt.Sprintf("metric %q (was %s, now %s)", metric, prev, identity))
		}
	}
	if len(redefined) > 0 {
		slices.Sort(redefined)
		if policy == DuplicatePolicyReject {
			return reject(fmt.Errorf("reload would redefine existing metric descriptors (use the %q policy to allow this): %s", DuplicatePolicyReplace, strings.Join(redefined, "; ")))
		}
		m.logger.Info("collector reload replaces existing metric descriptors", "metrics", strings.Join(redefined, "; "))
	}

	active := slices.Sorted(maps.Keys(collectors))
	m.genID++
	gen := &generation{
		id:         m.genID,
		collectors: &NodeCollector{Collectors: collectors, logger: m.logger},
		profiles:   profiles,
		names:      active,
	}
	m.current.Store(gen)
	m.state.Store(&managerState{
		generation:       gen.id,
		reloaded:         true,
		lastReload:       time.Now(),
		lastSuccess:      len(excluded) == 0,
		validationErrors: excluded,
		activeCollectors: active,
	})
	if len(excluded) > 0 {
		m.logger.Warn("collector reload excluded collectors that failed validation", "excluded", excluded)
	}
	m.logger.Info("collector reload applied", "generation", gen.id, "collectors", strings.Join(active, ","))
	return &ReloadResult{
		Applied:    true,
		Generation: gen.id,
		Active:     active,
		Excluded:   excluded,
	}, nil
}

// Describe implements prometheus.Collector.
func (m *Manager) Describe(ch chan<- *prometheus.Desc) {
	ch <- scrapeDurationDesc
	ch <- scrapeSuccessDesc
	ch <- collectorGenerationDesc
	ch <- collectorLastReloadSuccessDesc
	ch <- collectorLastReloadTimestampDesc
	ch <- collectorValidationErrorDesc
	ch <- collectorActiveDesc
}

// Collect implements prometheus.Collector. It scrapes exactly one collector
// generation: the one that was active when the scrape started.
func (m *Manager) Collect(ch chan<- prometheus.Metric) {
	m.current.Load().collectors.Collect(ch)

	st := m.state.Load()
	ch <- prometheus.MustNewConstMetric(collectorGenerationDesc, prometheus.GaugeValue, float64(st.generation))
	for _, name := range st.activeCollectors {
		ch <- prometheus.MustNewConstMetric(collectorActiveDesc, prometheus.GaugeValue, 1, name)
	}
	if !st.reloaded {
		return
	}
	var success float64
	if st.lastSuccess {
		success = 1
	}
	ch <- prometheus.MustNewConstMetric(collectorLastReloadSuccessDesc, prometheus.GaugeValue, success)
	ch <- prometheus.MustNewConstMetric(collectorLastReloadTimestampDesc, prometheus.GaugeValue, float64(st.lastReload.Unix()))
	for _, name := range slices.Sorted(maps.Keys(st.validationErrors)) {
		ch <- prometheus.MustNewConstMetric(collectorValidationErrorDesc, prometheus.GaugeValue, 1, name)
	}
}

// mergeIdentities unions the validated descriptor identities of the named
// collectors. It reports every metric name that two collectors describe
// differently as a conflict.
func mergeIdentities(profiles map[string]collectorProfile, names []string) (map[string]descriptorIdentity, []string) {
	identities := make(map[string]descriptorIdentity)
	owners := make(map[string]string)
	var conflicts []string
	for _, name := range names {
		profile := profiles[name]
		if !profile.validated {
			continue
		}
		for metric, identity := range profile.descriptors {
			prev, seen := identities[metric]
			if !seen {
				identities[metric] = identity
				owners[metric] = name
				continue
			}
			if !prev.equal(identity) {
				conflicts = append(conflicts, fmt.Sprintf("metric %q is described differently by collectors %q and %q", metric, owners[metric], name))
			}
		}
	}
	slices.Sort(conflicts)
	return identities, conflicts
}

// instantiateCollector returns the shared instance for a collector, creating
// it through its registered factory on first use.
func instantiateCollector(name string, logger *slog.Logger) (Collector, error) {
	factory, ok := factories[name]
	if !ok {
		return nil, fmt.Errorf("unknown collector: %s", name)
	}
	initiatedCollectorsMtx.Lock()
	defer initiatedCollectorsMtx.Unlock()
	if c, ok := initiatedCollectors[name]; ok {
		return c, nil
	}
	c, err := factory(logger.With("collector", name))
	if err != nil {
		return nil, err
	}
	initiatedCollectors[name] = c
	return c, nil
}

// profileCollector validates a collector's descriptors by running a probe
// scrape and gathering the result through a throwaway registry, which
// enforces the Prometheus descriptor consistency rules. It returns the
// descriptor identity of every metric the collector exposes.
func profileCollector(c Collector) (map[string]descriptorIdentity, error) {
	metrics, err := scrapeMetrics(c)
	if err != nil {
		return nil, fmt.Errorf("probe scrape failed: %w", err)
	}
	registry := prometheus.NewRegistry()
	if err := registry.Register(playbackCollector{metrics: metrics}); err != nil {
		return nil, fmt.Errorf("descriptor registration failed: %w", err)
	}
	families, err := registry.Gather()
	if err != nil {
		return nil, fmt.Errorf("descriptor consistency check failed: %w", err)
	}
	profile := make(map[string]descriptorIdentity, len(families))
	for _, family := range families {
		labelNames := map[string]struct{}{}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				labelNames[label.GetName()] = struct{}{}
			}
		}
		profile[family.GetName()] = descriptorIdentity{
			help:       family.GetHelp(),
			typ:        family.GetType().String(),
			labelNames: slices.Sorted(maps.Keys(labelNames)),
		}
	}
	return profile, nil
}

// scrapeMetrics runs one Update pass and returns the collected metrics.
func scrapeMetrics(c Collector) ([]prometheus.Metric, error) {
	ch := make(chan prometheus.Metric)
	var metrics []prometheus.Metric
	var wg sync.WaitGroup
	wg.Go(func() {
		for metric := range ch {
			metrics = append(metrics, metric)
		}
	})
	err := c.Update(ch)
	close(ch)
	wg.Wait()
	return metrics, err
}

// playbackCollector replays previously collected metrics so they can be
// gathered through a registry for consistency validation.
type playbackCollector struct {
	metrics []prometheus.Metric
}

// Describe implements prometheus.Collector. It sends no descriptors, making
// the collector unchecked at registration time; consistency is enforced by
// the gather instead.
func (playbackCollector) Describe(chan<- *prometheus.Desc) {}

// Collect implements prometheus.Collector.
func (p playbackCollector) Collect(ch chan<- prometheus.Metric) {
	for _, metric := range p.metrics {
		ch <- metric
	}
}

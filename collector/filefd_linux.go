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

//go:build !nofilefd

package collector

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	fileFDStatSubsystem = "filefd"

	defaultProcessFDTimeout = 2 * time.Second
	defaultProcessFDMax     = 1 << 20
)

var (
	fileFDProcessStats = kingpin.Flag(
		"collector.filefd.process-stats",
		"Enable consistent per-process open file descriptor counting from /proc/<pid>/fd.",
	).Default("false").Bool()
	fileFDProcessTimeout = kingpin.Flag(
		"collector.filefd.process-timeout",
		"Fixed time budget for one per-process fd scan. After it expires, no further proc entries are accessed.",
	).Default(defaultProcessFDTimeout.String()).Duration()
	fileFDProcessMaxFDs = kingpin.Flag(
		"collector.filefd.process-max-fds",
		"Maximum number of fd entries read for a single process. 0 disables the cap.",
	).Default(strconv.Itoa(defaultProcessFDMax)).Int()

	// Alternative flag spellings kept as hidden compatibility aliases. They
	// write through the same effective settings in NewFileFDStatCollector.
	fileFDProcessScanAlias = kingpin.Flag(
		"collector.filefd.process-scan",
		"Enable per-process open file descriptor counting from /proc/<pid>/fd.",
	).Hidden().Bool()
	fileFDProcessScanTimeoutAlias = kingpin.Flag(
		"collector.filefd.process-scan.timeout",
		"Alias for --collector.filefd.process-timeout.",
	).Hidden().Duration()
	fileFDProcessScanMaxFDsAlias = kingpin.Flag(
		"collector.filefd.process-scan.max-fds-per-process",
		"Alias for --collector.filefd.process-max-fds.",
	).Hidden().Int()
)

type fileFDStatCollector struct {
	logger *slog.Logger

	processStats  bool
	scanTimeout   time.Duration
	maxFDsPerProc int

	// procSource and now support the source-level scan API and deterministic
	// deadline tests. Production uses realProcFDSource.
	procSource procFDSource
	now        func() time.Time

	openFds         *prometheus.Desc
	scanTargets     *prometheus.Desc
	scanSkipped     *prometheus.Desc
	processSkipped  *prometheus.Desc
	scanTimeoutDesc *prometheus.Desc
}

func init() {
	registerCollector(fileFDStatSubsystem, defaultEnabled, NewFileFDStatCollector)
}

// NewFileFDStatCollector returns a new Collector exposing file-nr stats.
func NewFileFDStatCollector(logger *slog.Logger) (Collector, error) {
	timeout := *fileFDProcessTimeout
	if *fileFDProcessScanTimeoutAlias > 0 {
		timeout = *fileFDProcessScanTimeoutAlias
	}
	maxFDs := *fileFDProcessMaxFDs
	if *fileFDProcessScanMaxFDsAlias > 0 {
		maxFDs = *fileFDProcessScanMaxFDsAlias
	}

	source := &realProcFDSource{mountRoot: *procPath}
	return newFileFDStatCollector(
		logger,
		source,
		*fileFDProcessStats || *fileFDProcessScanAlias,
		timeout,
		maxFDs,
	), nil
}

// newFileFDStatCollector builds the collector with an injectable proc source.
// It is primarily useful for deterministic tests.
func newFileFDStatCollector(logger *slog.Logger, source procFDSource, processStats bool, scanTimeout time.Duration, maxFDsPerProc int) *fileFDStatCollector {
	if logger == nil {
		logger = slog.Default()
	}
	if source == nil {
		source = &realProcFDSource{mountRoot: *procPath}
	}

	return &fileFDStatCollector{
		logger:        logger,
		processStats:  processStats,
		scanTimeout:   scanTimeout,
		maxFDsPerProc: maxFDsPerProc,
		procSource:    source,
		now:           time.Now,
		openFds: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, fileFDStatSubsystem, "process_open_fds"),
			"Number of open file descriptors of the process.",
			[]string{"pid"}, nil,
		),
		scanTargets: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, fileFDStatSubsystem, "process_scan_processes"),
			"Number of processes in the fixed snapshot used by one file descriptor scan.",
			nil, nil,
		),
		scanSkipped: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, fileFDStatSubsystem, "process_scan_skipped"),
			"Number of snapshot processes skipped by the file descriptor scan, by stable reason.",
			[]string{"reason"}, nil,
		),
		processSkipped: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, fileFDStatSubsystem, "process_skipped"),
			"Whether a snapshot process was skipped, with its PID and stable reason.",
			[]string{"pid", "reason"}, nil,
		),
		scanTimeoutDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, fileFDStatSubsystem, "process_scan_timeout"),
			"Whether the per-process file descriptor scan stopped because its fixed deadline expired.",
			nil, nil,
		),
	}
}

func (c *fileFDStatCollector) Update(ch chan<- prometheus.Metric) error {
	root := *procPath
	if c.procSource != nil {
		root = c.procSource.root()
	}

	fileFDStat, err := parseFileFDStats(filepath.Join(root, "sys/fs/file-nr"))
	if err != nil {
		return fmt.Errorf("couldn't get file-nr: %w", err)
	}
	for name, value := range fileFDStat {
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return fmt.Errorf("invalid value %s in file-nr: %w", value, err)
		}
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName(namespace, fileFDStatSubsystem, name),
				fmt.Sprintf("File descriptor statistics: %s.", name),
				nil, nil,
			),
			prometheus.GaugeValue, v,
		)
	}

	if c.processStats {
		c.updateProcessFDs(ch)
	}
	return nil
}

// collectProcessFDs uses the batched/deadline-aware scanner for the normal
// procfs source. Injectable test sources retain the source-oriented accounting
// API used by focused scan tests.
func (c *fileFDStatCollector) collectProcessFDs() procFDScanResult {
	if _, ok := c.procSource.(*realProcFDSource); !ok {
		return c.scanProcessFDs()
	}

	scanner, err := newFDScanner(c.procSource.root(), 0, c.maxFDsPerProc, c.logger)
	if err != nil {
		c.logger.Debug("failed to create file descriptor scanner", "err", err)
		return newEmptyProcFDScanResult()
	}
	scanner.now = c.now
	if c.scanTimeout > 0 {
		scanner.deadline = c.now().Add(c.scanTimeout)
	}

	snapshot, err := scanner.scan()
	if err != nil {
		c.logger.Debug("failed to scan file descriptors", "err", err)
		return newEmptyProcFDScanResult()
	}

	result := newEmptyProcFDScanResult()
	result.targets = len(snapshot.PIDs)
	result.timedOut = snapshot.TimedOut
	for _, pid := range snapshot.PIDs {
		switch {
		case snapshot.Skipped[pid] != "":
			reason := coreSkipReasonToCompat(snapshot.Skipped[pid])
			result.skipped[reason]++
			result.skipReasons[pid] = reason
		case snapshot.Capped[pid]:
			result.skipped[fileFDSkipLimit]++
			result.skipReasons[pid] = fileFDSkipLimit
		default:
			result.openFDs[pid] = snapshot.Counts[pid]
		}
	}
	return result
}

func (c *fileFDStatCollector) updateProcessFDs(ch chan<- prometheus.Metric) {
	result := c.collectProcessFDs()

	ch <- prometheus.MustNewConstMetric(c.scanTargets, prometheus.GaugeValue, float64(result.targets))
	ch <- prometheus.MustNewConstMetric(c.scanTimeoutDesc, prometheus.GaugeValue, boolToFloat64(result.timedOut))

	for pid, count := range result.openFDs {
		ch <- prometheus.MustNewConstMetric(c.openFds, prometheus.GaugeValue, float64(count), strconv.Itoa(pid))
	}

	for _, reason := range []string{
		fileFDSkipVanished,
		fileFDSkipPermission,
		fileFDSkipLimit,
		fileFDSkipDeadline,
		fileFDSkipError,
	} {
		ch <- prometheus.MustNewConstMetric(c.scanSkipped, prometheus.GaugeValue, float64(result.skipped[reason]), reason)
	}

	for pid, reason := range result.skipReasons {
		coreReason := compatSkipReasonToCore(reason)
		ch <- prometheus.MustNewConstMetric(
			c.processSkipped, prometheus.GaugeValue, 1,
			strconv.Itoa(pid), coreReason,
		)
	}
}

func boolToFloat64(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

func parseFileFDStats(filename string) (map[string]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	parts := bytes.Split(bytes.TrimSpace(content), []byte("\t"))
	if len(parts) < 3 {
		return nil, fmt.Errorf("unexpected number of file stats in %q", filename)
	}

	fileFDStat := map[string]string{}
	// The file-nr proc is only 1 line with 3 values.
	fileFDStat["allocated"] = string(parts[0])
	// The second value is skipped as it will always be zero in linux 2.6.
	fileFDStat["maximum"] = string(parts[2])

	return fileFDStat, nil
}

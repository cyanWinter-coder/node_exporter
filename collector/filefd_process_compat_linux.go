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

//go:build !nofilefd

package collector

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// Stable reasons used by the source-oriented scan API. The deadline and limit
// labels intentionally describe why this accounting layer did not produce a
// count; the scanner-level vocabulary uses fdSkipTimeout for timeouts.
const (
	fileFDSkipVanished   = fdSkipVanished
	fileFDSkipPermission = fdSkipPermission
	fileFDSkipError      = fdSkipError
	fileFDSkipLimit      = "fd_limit_exceeded"
	fileFDSkipDeadline   = "deadline_exceeded"
)

// procFDSource is the narrow procfs operations needed by a scan. Tests can
// replace it with an in-memory or temporary-directory implementation.
type procFDSource interface {
	root() string
	listPIDs() ([]int, error)
	countFDs(pid, maxFDs int) (count int, fdLimitHit bool, err error)
}

// procFDScanResult is the fixed-snapshot accounting for one scan.
type procFDScanResult struct {
	openFDs     map[int]int
	skipped     map[string]int
	skipReasons map[int]string
	targets     int
	timedOut    bool
}

// scanProcessFDs lists processes once, then visits only that PID set. Every
// snapshot PID is represented either in openFDs or in one skip bucket.
func (c *fileFDStatCollector) scanProcessFDs() procFDScanResult {
	result := newEmptyProcFDScanResult()

	pids, err := c.procSource.listPIDs()
	if err != nil {
		c.logger.Debug("failed to list processes for file descriptor scan", "err", err)
		return result
	}
	sort.Ints(pids)
	result.targets = len(pids)

	var deadline time.Time
	if c.scanTimeout > 0 {
		deadline = c.now().Add(c.scanTimeout)
	}

	for i, pid := range pids {
		if !deadline.IsZero() && !c.now().Before(deadline) {
			result.timedOut = true
			result.skipped[fileFDSkipDeadline] += len(pids) - i
			for _, remainingPID := range pids[i:] {
				result.skipReasons[remainingPID] = fileFDSkipDeadline
			}
			break
		}

		count, limitHit, err := c.procSource.countFDs(pid, c.maxFDsPerProc)
		if err != nil {
			reason := classifyProcFDError(err)
			result.skipped[reason]++
			result.skipReasons[pid] = reason
			c.logger.Debug("skipping process during file descriptor scan",
				"pid", pid, "reason", reason, "err", err)
			continue
		}
		if limitHit {
			result.skipped[fileFDSkipLimit]++
			result.skipReasons[pid] = fileFDSkipLimit
			continue
		}
		result.openFDs[pid] = count
	}

	return result
}

// countScanned checks snapshot accounting: reported plus skipped PIDs.
func countScanned(result procFDScanResult) int {
	scanned := len(result.openFDs)
	for _, n := range result.skipped {
		scanned += n
	}
	return scanned
}

// classifyProcFDError maps proc access errors to stable skip reasons.
func classifyProcFDError(err error) string {
	switch {
	case errors.Is(err, os.ErrNotExist), errors.Is(err, os.ErrDeadlineExceeded):
		return fileFDSkipVanished
	case errors.Is(err, os.ErrPermission):
		return fileFDSkipPermission
	default:
		return fileFDSkipError
	}
}

func compatSkipReasonToCore(reason string) string {
	if reason == fileFDSkipDeadline {
		return fdSkipTimeout
	}
	return reason
}

func coreSkipReasonToCompat(reason string) string {
	if reason == fdSkipTimeout {
		return fileFDSkipDeadline
	}
	return reason
}

func newEmptyProcFDScanResult() procFDScanResult {
	return procFDScanResult{
		openFDs:     make(map[int]int),
		skipReasons: make(map[int]string),
		skipped: map[string]int{
			fileFDSkipVanished:   0,
			fileFDSkipPermission: 0,
			fileFDSkipLimit:      0,
			fileFDSkipDeadline:   0,
			fileFDSkipError:      0,
		},
	}
}

// realProcFDSource reads a procfs-shaped directory from the local filesystem.
type realProcFDSource struct {
	mountRoot string
}

func (s *realProcFDSource) root() string {
	return s.mountRoot
}

func (s *realProcFDSource) listPIDs() ([]int, error) {
	entries, err := os.ReadDir(s.mountRoot)
	if err != nil {
		return nil, err
	}

	pids := make([]int, 0, len(entries))
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			// Non-numeric proc entries such as sys, self, and net are not PIDs.
			continue
		}
		pids = append(pids, pid)
	}
	return pids, nil
}

// countFDs counts names in <root>/<pid>/fd. It reads in bounded batches and
// performs at most one probe read after the cap, so an oversized directory
// cannot be drained.
func (s *realProcFDSource) countFDs(pid, maxFDs int) (int, bool, error) {
	dir, err := os.Open(filepath.Join(s.mountRoot, strconv.Itoa(pid), "fd"))
	if err != nil {
		return 0, false, err
	}
	defer dir.Close()

	if maxFDs <= 0 {
		names, err := dir.Readdirnames(-1)
		if err != nil && !errors.Is(err, io.EOF) {
			return 0, false, err
		}
		return len(names), false, nil
	}

	count := 0
	for {
		if count >= maxFDs {
			// Probe one entry to distinguish an exact boundary from a
			// directory that still has entries beyond the cap.
			names, err := dir.Readdirnames(1)
			switch {
			case len(names) > 0:
				return 0, true, nil
			case errors.Is(err, io.EOF):
				return count, false, nil
			case err != nil:
				return 0, false, err
			default:
				return count, false, nil
			}
		}

		limit := min(fdScanBatch, maxFDs-count)
		names, err := dir.Readdirnames(limit)
		count += len(names)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return count, false, nil
			}
			return 0, false, err
		}
		if len(names) == 0 {
			return count, false, nil
		}
		if count > maxFDs {
			return 0, true, nil
		}
	}
}

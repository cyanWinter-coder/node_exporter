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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/procfs"
)

const (
	// fdScanBatch is the maximum number of directory entries consumed in one
	// readdir call. Keeping batches small makes deadline and per-process cap
	// checks responsive even for an unusually large fd directory.
	fdScanBatch = 256
)

// Stable reasons why a process from the consistent snapshot is not reported
// with an open file descriptor count. These values are exposed in metrics and
// must remain stable.
const (
	fdSkipVanished   = "vanished"
	fdSkipPermission = "permission_denied"
	fdSkipTimeout    = "timeout"
	fdSkipError      = "error"
)

// procFDSnapshot is the result of one budgeted scan. Every PID in PIDs appears
// exactly once in either Counts or Skipped. When Capped[pid] is true, Counts
// contains the configured per-process limit rather than the exact total.
type procFDSnapshot struct {
	PIDs       []int
	Counts     map[int]int
	Capped     map[int]bool
	Skipped    map[int]string
	SkipCounts map[string]int
	TimedOut   bool
}

// fdScanner counts open file descriptors under a procfs root using a fixed
// process snapshot, a fixed deadline, and a per-process read budget.
type fdScanner struct {
	root     string
	maxFDs   int
	deadline time.Time
	now      func() time.Time
	logger   *slog.Logger

	// snapshot lists processes once at the beginning of a scan. It is
	// replaceable in tests.
	snapshot func() (procfs.Procs, error)
	// openDir opens one process fd directory. It is replaceable in tests.
	openDir func(path string) (*os.File, error)

	timedOut bool
}

// newFDScanner creates a scanner rooted at procRoot. A zero timeout disables
// the deadline and a zero maxFDs disables the per-process cap.
func newFDScanner(procRoot string, timeout time.Duration, maxFDs int, logger *slog.Logger) (*fdScanner, error) {
	fs, err := procfs.NewFS(procRoot)
	if err != nil {
		return nil, fmt.Errorf("cannot open procfs at %q: %w", procRoot, err)
	}
	if logger == nil {
		logger = slog.Default()
	}

	s := &fdScanner{
		root:     procRoot,
		maxFDs:   max(maxFDs, 0),
		logger:   logger,
		now:      time.Now,
		snapshot: fs.AllProcs,
		openDir:  os.Open,
	}
	if timeout > 0 {
		s.deadline = s.now().Add(timeout)
	}
	return s, nil
}

// scan takes the fixed process snapshot and visits only those PIDs. Processes
// created after the snapshot cannot enter the result. Every snapshot member is
// accounted for exactly once.
func (s *fdScanner) scan() (*procFDSnapshot, error) {
	procs, err := s.snapshot()
	if err != nil {
		return nil, fmt.Errorf("cannot snapshot process set: %w", err)
	}

	pids := make([]int, 0, len(procs))
	for _, proc := range procs {
		pids = append(pids, proc.PID)
	}
	slices.Sort(pids)

	snap := &procFDSnapshot{
		PIDs:       pids,
		Counts:     make(map[int]int, len(pids)),
		Capped:     make(map[int]bool),
		Skipped:    make(map[int]string),
		SkipCounts: make(map[string]int, 4),
	}

	for i, pid := range pids {
		if s.expired() {
			s.markTimeout(snap, pids[i:]...)
			break
		}

		count, capped, reason := s.countProcessFDs(pid)
		switch reason {
		case "":
			snap.Counts[pid] = count
			snap.Capped[pid] = capped
		case fdSkipTimeout:
			s.markTimeout(snap, pid)
			// Do not open any proc entry for the remaining snapshot members.
			s.markTimeout(snap, pids[i+1:]...)
			snap.TimedOut = true
			return snap, nil
		default:
			snap.Skipped[pid] = reason
			snap.SkipCounts[reason]++
		}
	}

	snap.TimedOut = s.timedOut
	return snap, nil
}

// countProcessFDs enumerates one /proc/<pid>/fd directory. It never reads more
// than one entry beyond maxFDs and checks the shared deadline between batches.
func (s *fdScanner) countProcessFDs(pid int) (count int, capped bool, reason string) {
	if s.expired() {
		return 0, false, fdSkipTimeout
	}

	fdDir := filepath.Join(s.root, strconv.Itoa(pid), "fd")
	dir, err := s.openDir(fdDir)
	if err != nil {
		return 0, false, s.classifyError(err)
	}
	defer dir.Close()

	for {
		if s.expired() {
			return 0, false, fdSkipTimeout
		}

		// When the end of the cap window is close, request only enough
		// entries to reach the cap plus one probe entry. Far from the cap,
		// bounded batches keep deadline checks responsive.
		limit := fdScanBatch
		if s.maxFDs > 0 && s.maxFDs-count < fdScanBatch {
			limit = s.maxFDs - count + 1
		}

		names, err := dir.Readdirnames(limit)
		count += len(names)

		if s.maxFDs > 0 && count > s.maxFDs {
			return s.maxFDs, true, ""
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return count, false, ""
			}
			return 0, false, s.classifyReadError(pid, err)
		}
		if len(names) == 0 {
			// Defensive fallback for a filesystem implementation that returns
			// neither names nor io.EOF.
			return count, false, ""
		}
	}
}

func (s *fdScanner) markTimeout(snap *procFDSnapshot, pids ...int) {
	if len(pids) == 0 {
		return
	}
	s.timedOut = true
	for _, pid := range pids {
		if _, alreadyDone := snap.Counts[pid]; alreadyDone {
			continue
		}
		if _, alreadySkipped := snap.Skipped[pid]; alreadySkipped {
			continue
		}
		snap.Skipped[pid] = fdSkipTimeout
		snap.SkipCounts[fdSkipTimeout]++
	}
}

// expired reports whether the fixed scan deadline has passed. A zero deadline
// means the scan has no time budget.
func (s *fdScanner) expired() bool {
	if s.deadline.IsZero() {
		return false
	}
	if s.now().Before(s.deadline) {
		return false
	}
	s.timedOut = true
	return true
}

func (s *fdScanner) classifyError(err error) string {
	switch {
	case errors.Is(err, os.ErrNotExist), errors.Is(err, syscall.ENOENT), errors.Is(err, syscall.ESRCH):
		return fdSkipVanished
	case errors.Is(err, os.ErrPermission), errors.Is(err, syscall.EACCES), errors.Is(err, syscall.EPERM):
		return fdSkipPermission
	case errors.Is(err, os.ErrDeadlineExceeded):
		return fdSkipTimeout
	default:
		s.logger.Debug("error accessing process fd directory", "err", err)
		return fdSkipError
	}
}

func (s *fdScanner) classifyReadError(pid int, err error) string {
	switch {
	case errors.Is(err, os.ErrNotExist), errors.Is(err, syscall.ENOENT), errors.Is(err, syscall.ESRCH), errors.Is(err, os.ErrClosed):
		return fdSkipVanished
	case errors.Is(err, os.ErrPermission), errors.Is(err, syscall.EACCES), errors.Is(err, syscall.EPERM):
		return fdSkipPermission
	case errors.Is(err, os.ErrDeadlineExceeded):
		return fdSkipTimeout
	default:
		s.logger.Debug("error reading process fd directory", "pid", pid, "err", err)
		return fdSkipError
	}
}

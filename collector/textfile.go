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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

var (
	textFileDirectories = kingpin.Flag("collector.textfile.directory", "Directory to read text files with metrics from, supports glob matching. (repeatable)").Default("").Strings()
	textFileSizeLimit   = kingpin.Flag("collector.textfile.file-size-limit", "Maximum size in bytes of a single textfile metrics file. Larger files are skipped and reported via node_textfile_scrape_size_limit_exceeded. 0 disables the limit.").Default("0").Bytes()
	mtimeDesc           = prometheus.NewDesc(
		"node_textfile_mtime_seconds",
		"Unixtime mtime of textfiles successfully read.",
		[]string{"file"},
		nil,
	)
	sizeLimitExceededDesc = prometheus.NewDesc(
		"node_textfile_scrape_size_limit_exceeded",
		"1 if a file exceeded the limit set by --collector.textfile.file-size-limit during the last scrape and was skipped, 0 otherwise. Only exported when the limit is enabled.",
		nil,
		nil,
	)

	// errFileChanged marks a read that raced with a replace or modification of
	// the file, so the scrape must not merge content from different
	// generations of the same file.
	errFileChanged = errors.New("file changed while being read")
	// errSizeLimitExceeded marks a file that is larger than the configured
	// byte budget (--collector.textfile.file-size-limit).
	errSizeLimitExceeded = errors.New("file size limit exceeded")
)

type textFileCollector struct {
	paths []string
	// Only set for testing to get predictable output.
	mtime *float64
	// sizeLimit is the per-file byte budget for parsing; 0 disables it.
	sizeLimit uint64
	// statAfterRead replaces the post-read os.Stat; only set for testing.
	statAfterRead func(path string) (os.FileInfo, error)
	logger        *slog.Logger
}

func init() {
	registerCollector("textfile", defaultEnabled, NewTextFileCollector)
}

// NewTextFileCollector returns a new Collector exposing metrics read from files
// in the given textfile directory.
func NewTextFileCollector(logger *slog.Logger) (Collector, error) {
	c := &textFileCollector{
		paths:     *textFileDirectories,
		sizeLimit: uint64(*textFileSizeLimit),
		logger:    logger,
	}
	return c, nil
}

func convertMetricFamily(metricFamily *dto.MetricFamily, ch chan<- prometheus.Metric, logger *slog.Logger) {
	var valType prometheus.ValueType
	var val float64

	allLabelNames := map[string]struct{}{}
	for _, metric := range metricFamily.Metric {
		labels := metric.GetLabel()
		for _, label := range labels {
			if _, ok := allLabelNames[label.GetName()]; !ok {
				allLabelNames[label.GetName()] = struct{}{}
			}
		}
	}

	for _, metric := range metricFamily.Metric {
		if metric.TimestampMs != nil {
			logger.Warn("Ignoring unsupported custom timestamp on textfile collector metric", "metric", metric)
		}

		labels := metric.GetLabel()
		var names []string
		var values []string
		for _, label := range labels {
			names = append(names, label.GetName())
			values = append(values, label.GetValue())
		}

		for k := range allLabelNames {
			if !slices.Contains(names, k) {
				names = append(names, k)
				values = append(values, "")
			}
		}

		metricType := metricFamily.GetType()
		switch metricType {
		case dto.MetricType_COUNTER:
			valType = prometheus.CounterValue
			val = metric.Counter.GetValue()

		case dto.MetricType_GAUGE:
			valType = prometheus.GaugeValue
			val = metric.Gauge.GetValue()

		case dto.MetricType_UNTYPED:
			valType = prometheus.UntypedValue
			val = metric.Untyped.GetValue()

		case dto.MetricType_SUMMARY:
			quantiles := map[float64]float64{}
			for _, q := range metric.Summary.Quantile {
				quantiles[q.GetQuantile()] = q.GetValue()
			}
			ch <- prometheus.MustNewConstSummary(
				prometheus.NewDesc(
					*metricFamily.Name,
					metricFamily.GetHelp(),
					names, nil,
				),
				metric.Summary.GetSampleCount(),
				metric.Summary.GetSampleSum(),
				quantiles, values...,
			)
		case dto.MetricType_HISTOGRAM:
			buckets := map[float64]uint64{}
			for _, b := range metric.Histogram.Bucket {
				buckets[b.GetUpperBound()] = b.GetCumulativeCount()
			}
			ch <- prometheus.MustNewConstHistogram(
				prometheus.NewDesc(
					*metricFamily.Name,
					metricFamily.GetHelp(),
					names, nil,
				),
				metric.Histogram.GetSampleCount(),
				metric.Histogram.GetSampleSum(),
				buckets, values...,
			)
		default:
			panic("unknown metric type")
		}
		if metricType == dto.MetricType_GAUGE || metricType == dto.MetricType_COUNTER || metricType == dto.MetricType_UNTYPED {
			ch <- prometheus.MustNewConstMetric(
				prometheus.NewDesc(
					*metricFamily.Name,
					metricFamily.GetHelp(),
					names, nil,
				),
				valType, val, values...,
			)
		}
	}
}

func (c *textFileCollector) exportMTimes(mtimes map[string]time.Time, ch chan<- prometheus.Metric) {
	if len(mtimes) == 0 {
		return
	}

	// Export the mtimes of the successful files.
	// Sorting is needed for predictable output comparison in tests.
	filepaths := make([]string, 0, len(mtimes))
	for path := range mtimes {
		filepaths = append(filepaths, path)
	}
	sort.Strings(filepaths)

	for _, path := range filepaths {
		mtime := float64(mtimes[path].UnixNano() / 1e9)
		if c.mtime != nil {
			mtime = *c.mtime
		}
		ch <- prometheus.MustNewConstMetric(mtimeDesc, prometheus.GaugeValue, mtime, path)
	}
}

// Update implements the Collector interface.
func (c *textFileCollector) Update(ch chan<- prometheus.Metric) error {
	// Iterate over files and accumulate their metrics, but also track any
	// parsing errors so an error metric can be reported. Each scrape builds a
	// complete snapshot of the directory: metrics of files that disappeared or
	// failed to parse are simply not part of it, so nothing stale lingers.
	var errored bool
	var sizeLimitExceeded bool
	var parsedFamilies []*dto.MetricFamily
	metricsNamesToFiles := map[string][]string{}
	metricsNamesToHelpTexts := map[string][2]string{}

	paths := []string{}
	for _, glob := range c.paths {
		ps, err := filepath.Glob(glob)
		if err != nil || len(ps) == 0 {
			// not glob or not accessible path either way assume single
			// directory and let os.ReadDir handle it
			ps = []string{glob}
		}
		paths = append(paths, ps...)
	}

	mtimes := make(map[string]time.Time)
	for _, path := range paths {
		files, err := os.ReadDir(path)
		if err != nil && path != "" {
			errored = true
			c.logger.Error("failed to read textfile collector directory", "path", path, "err", err)
		}

		for _, f := range files {
			metricsFilePath := filepath.Join(path, f.Name())
			if !strings.HasSuffix(f.Name(), ".prom") {
				continue
			}

			mtime, families, err := c.processFile(path, f.Name())

			for _, mf := range families {
				// Check for metrics with inconsistent help texts and take the first help text occurrence.
				if helpTexts, seen := metricsNamesToHelpTexts[*mf.Name]; seen {
					if mf.Help != nil && helpTexts[0] != *mf.Help || helpTexts[1] != "" {
						if mf.Help != nil {
							metricsNamesToHelpTexts[*mf.Name] = [2]string{helpTexts[0], *mf.Help}
						}
						errored = true
						c.logger.Error("inconsistent metric help text",
							"metric", *mf.Name,
							"original_help_text", helpTexts[0],
							"new_help_text", mf.GetHelp(),
							// Only the first file path will be recorded in case of two or more inconsistent help texts.
							"file", metricsNamesToFiles[*mf.Name][0])
						continue
					}
				}
				if mf.Help != nil {
					metricsNamesToHelpTexts[*mf.Name] = [2]string{*mf.Help}
				}
				metricsNamesToFiles[*mf.Name] = append(metricsNamesToFiles[*mf.Name], metricsFilePath)
				parsedFamilies = append(parsedFamilies, mf)
			}

			if err != nil {
				errored = true
				if errors.Is(err, errSizeLimitExceeded) {
					sizeLimitExceeded = true
				}
				c.logger.Error("failed to collect textfile data", "file", f.Name(), "err", err)
				continue
			}

			mtimes[metricsFilePath] = *mtime
		}
	}

	mfHelp := make(map[string]*string)
	for _, mf := range parsedFamilies {
		if mf.Help == nil {
			if help, ok := mfHelp[*mf.Name]; ok {
				mf.Help = help
				continue
			}
			help := fmt.Sprintf("Metric read from %s", strings.Join(metricsNamesToFiles[*mf.Name], ", "))
			mf.Help = &help
			mfHelp[*mf.Name] = &help
		}
	}

	for _, mf := range parsedFamilies {
		convertMetricFamily(mf, ch, c.logger)
	}

	c.exportMTimes(mtimes, ch)

	// Export if there were errors.
	var errVal float64
	if errored {
		errVal = 1.0
	}

	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			"node_textfile_scrape_error",
			"1 if there was an error opening or reading a file, 0 otherwise",
			nil, nil,
		),
		prometheus.GaugeValue, errVal,
	)

	// Only export the size limit metric when the limit is enabled, so that
	// scrapes without a configured budget stay unchanged.
	if c.sizeLimit > 0 {
		var sizeLimitVal float64
		if sizeLimitExceeded {
			sizeLimitVal = 1.0
		}
		ch <- prometheus.MustNewConstMetric(
			sizeLimitExceededDesc,
			prometheus.GaugeValue, sizeLimitVal,
		)
	}

	return nil
}

// processFile reads and parses a single file, returning its modification time
// on success. If the file is replaced or modified while it is being read, the
// read is retried once so that a scrape never merges content from different
// generations of the same file. On any error the returned families are nil,
// so a failed file contributes no metrics but does not affect other files.
func (c *textFileCollector) processFile(dir, name string) (*time.Time, map[string]*dto.MetricFamily, error) {
	path := filepath.Join(dir, name)
	var err error
	for attempt := 1; attempt <= 2; attempt++ {
		var mtime *time.Time
		var families map[string]*dto.MetricFamily
		mtime, families, err = c.readFileSnapshot(path)
		if err == nil {
			return mtime, families, nil
		}
		if !errors.Is(err, errFileChanged) {
			return nil, nil, err
		}
		c.logger.Warn("textfile changed while being read, retrying", "file", path, "attempt", attempt)
	}
	return nil, nil, fmt.Errorf("file %q did not stabilize after one retry: %w", path, err)
}

// readFileSnapshot reads and parses a single file once, verifying that the
// file's identity (device/inode) and modification generation (mtime, size)
// are unchanged between the open and the end of the parse.
func (c *textFileCollector) readFileSnapshot(path string) (*time.Time, map[string]*dto.MetricFamily, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open textfile data file %q: %w", path, err)
	}
	// Closed on every return path, so an aborted or retried scrape never
	// leaks the descriptor.
	defer f.Close()

	before, err := f.Stat()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to stat %q: %w", path, err)
	}

	// Bound the parse by the configured byte budget: the limited reader
	// allows one byte past the limit so an oversized file is detected even
	// when its truncated content would still parse.
	var r io.Reader = f
	var lr *io.LimitedReader
	if c.sizeLimit > 0 {
		lr = &io.LimitedReader{R: f, N: int64(c.sizeLimit) + 1}
		r = lr
	}

	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(r)

	// Check the budget before the parse error: once the limit is hit the
	// file is oversized regardless of whether the truncated content still
	// parses, and a parse failure is most likely caused by the truncation.
	if lr != nil && lr.N <= 0 {
		return nil, nil, fmt.Errorf("textfile data file %q is larger than the %d byte limit: %w", path, c.sizeLimit, errSizeLimitExceeded)
	}

	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse textfile data from %q: %w", path, err)
	}

	if hasTimestamps(families) {
		return nil, nil, fmt.Errorf("textfile %q contains unsupported client-side timestamps, skipping entire file", path)
	}

	// Verify that the path still refers to the same, unmodified file we just
	// parsed; a concurrent replace or modification means the content we read
	// may not match any single generation of the file.
	statAfter := os.Stat
	if c.statAfterRead != nil {
		statAfter = c.statAfterRead
	}
	after, err := statAfter(path)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to stat %q after reading: %w", path, err)
	}
	if !sameFileSnapshot(before, after) {
		return nil, nil, errFileChanged
	}

	t := after.ModTime()
	return &t, families, nil
}

// sameFileSnapshot reports whether before and after describe the same file
// (device and inode) with an unchanged modification generation (mtime and
// size).
func sameFileSnapshot(before, after os.FileInfo) bool {
	return os.SameFile(before, after) &&
		before.ModTime().Equal(after.ModTime()) &&
		before.Size() == after.Size()
}

// hasTimestamps returns true when metrics contain unsupported timestamps.
func hasTimestamps(parsedFamilies map[string]*dto.MetricFamily) bool {
	for _, mf := range parsedFamilies {
		for _, m := range mf.Metric {
			if m.TimestampMs != nil {
				return true
			}
		}
	}
	return false
}

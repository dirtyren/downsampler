package main

import (
    "encoding/json"
    "flag"
    "fmt"
    "io"
    "log"
    "math"
    "net/http"
    "net/url"
    "sort"
    "strconv"
    "strings"
    "sync"
    "time"
)

// DataPoint represents a time series data point
type DataPoint struct {
    Timestamp int64   `json:"timestamp"`
    Value     float64 `json:"value"`
}

// TimeSeries represents a complete time series with its labels
type TimeSeries struct {
    Metric     map[string]string `json:"metric"`
    DataPoints []DataPoint       `json:"datapoints"`
}

// VMResponse represents VictoriaMetrics query response
type VMResponse struct {
    Status string `json:"status"`
    Data   struct {
        ResultType string `json:"resultType"`
        Result     []struct {
            Metric map[string]string `json:"metric"`
            Values [][]interface{}   `json:"values"`
        } `json:"result"`
    } `json:"data"`
}

// VMSeriesResponse represents the series endpoint response
type VMSeriesResponse struct {
    Status string              `json:"status"`
    Data   []map[string]string `json:"data"`
}

// VMLabelValuesResponse represents the label values response
type VMLabelValuesResponse struct {
    Status string   `json:"status"`
    Data   []string `json:"data"`
}

// Config holds application configuration
type Config struct {
    VMBaseURL          string
    StartTime          time.Time
    EndTime            time.Time
    SummarizationFrame string        // "5m", "15m", "30m", "hour", "day", "month"
    FrameDuration      time.Duration // Parsed duration for custom frames
    BucketsPerFrame    int           // Number of LTTB buckets per timeframe
    BatchSize          int           // Number of series to process at once
    DryRun             bool
    DeleteOriginal     bool
    SuffixLabel        string // Label to add to summarized data
    MetricFilter       string // PromQL filter for metrics
    MaxPointsPerQuery  int    // Maximum points to fetch per query
    MetricPrefix       string // Prefix for new metric names
    UsePrefix          bool   // Whether to use prefix instead of suffix label
}

// Summary tracks the summarization statistics
type Summary struct {
    TotalSeries          int
    ProcessedSeries      int
    FailedSeries         int
    OriginalPoints       int64
    SummarizedPoints     int64
    EstimatedSpaceSaved  int64 // in bytes
    ProcessingTime       time.Duration
}

// VMClient handles VictoriaMetrics operations
type VMClient struct {
    baseURL string
    client  *http.Client
}

// NewVMClient creates a new VictoriaMetrics client
func NewVMClient(baseURL string) *VMClient {
    return &VMClient{
        baseURL: strings.TrimRight(baseURL, "/"),
        client: &http.Client{
            Timeout: 5 * time.Minute,
        },
    }
}

// GetMetricNames gets all metric names from VictoriaMetrics
func (vm *VMClient) GetMetricNames(start, end time.Time, filter string) ([]string, error) {
    params := url.Values{}
    params.Set("start", fmt.Sprintf("%d", start.Unix()))
    params.Set("end", fmt.Sprintf("%d", end.Unix()))
    
    // If filter is provided and is a specific metric name pattern, extract it
    metricPattern := ""
    if filter != "" && strings.Contains(filter, "__name__") {
        // Try to extract metric name pattern from filter
        parts := strings.Split(filter, "__name__=~")
        if len(parts) > 1 {
            // Extract the pattern between quotes
            pattern := strings.TrimSpace(parts[1])
            if strings.HasPrefix(pattern, `"`) {
                endIdx := strings.Index(pattern[1:], `"`)
                if endIdx > 0 {
                    metricPattern = pattern[1:endIdx+1]
                }
            }
        }
    }
    
    // If we have a specific pattern, use it; otherwise get all metrics
    if metricPattern != "" && metricPattern != ".+" && metricPattern != ".*" {
        params.Set("match[]", fmt.Sprintf("{__name__=~\"%s\"}", metricPattern))
    }
    
    labelValuesURL := fmt.Sprintf("%s/api/v1/label/__name__/values?%s", vm.baseURL, params.Encode())
    
    log.Printf("Fetching metric names from: %s", labelValuesURL)
    
    resp, err := vm.client.Get(labelValuesURL)
    if err != nil {
        return nil, fmt.Errorf("failed to fetch metric names: %w", err)
    }
    defer resp.Body.Close()
    
    if resp.StatusCode != http.StatusOK {
        body, _ := io.ReadAll(resp.Body)
        return nil, fmt.Errorf("label values request failed with status %d: %s", resp.StatusCode, string(body))
    }
    
    var labelResp VMLabelValuesResponse
    if err := json.NewDecoder(resp.Body).Decode(&labelResp); err != nil {
        return nil, fmt.Errorf("failed to decode label values response: %w", err)
    }
    
    log.Printf("Found %d metric names", len(labelResp.Data))
    return labelResp.Data, nil
}

// ListAllSeries gets all series matching the time range and filter
func (vm *VMClient) ListAllSeries(start, end time.Time, filter string) ([]map[string]string, error) {
    // If no filter is provided or filter is too broad, use metric-by-metric approach
    if filter == "" || filter == "{__name__=~\".+\"}" || filter == "{__name__=~\".*\"}" {
        return vm.listSeriesByMetrics(start, end, "")
    }
    
    // Otherwise, try direct query
    return vm.listSeriesDirect(start, end, filter)
}

// listSeriesByMetrics lists series by querying each metric name separately
func (vm *VMClient) listSeriesByMetrics(start, end time.Time, metricFilter string) ([]map[string]string, error) {
    // First, get all metric names
    metricNames, err := vm.GetMetricNames(start, end, metricFilter)
    if err != nil {
        return nil, fmt.Errorf("failed to get metric names: %w", err)
    }
    
    if len(metricNames) == 0 {
        return []map[string]string{}, nil
    }
    
    log.Printf("Found %d metric names, fetching series for each...", len(metricNames))
    
    allSeries := make([]map[string]string, 0)
    var mu sync.Mutex
    var wg sync.WaitGroup
    
    // Process metrics in batches to avoid overwhelming the server
    batchSize := 10
    semaphore := make(chan struct{}, batchSize)
    
    for i, metricName := range metricNames {
        wg.Add(1)
        go func(idx int, metric string) {
            defer wg.Done()
            
            // Acquire semaphore
            semaphore <- struct{}{}
            defer func() { <-semaphore }()
            
            // Progress indicator
            if idx%100 == 0 {
                log.Printf("Processing metric %d/%d: %s", idx+1, len(metricNames), metric)
            }
            
            // Get series for this specific metric
            series, err := vm.listSeriesDirect(start, end, metric)
            if err != nil {
                log.Printf("Warning: failed to get series for metric %s: %v", metric, err)
                return
            }
            
            // Add to results
            mu.Lock()
            allSeries = append(allSeries, series...)
            mu.Unlock()
        }(i, metricName)
    }
    
    wg.Wait()
    
    return allSeries, nil
}

// listSeriesDirect performs a direct series query
func (vm *VMClient) listSeriesDirect(start, end time.Time, filter string) ([]map[string]string, error) {
    params := url.Values{}
    params.Set("match[]", filter)
    params.Set("start", fmt.Sprintf("%d", start.Unix()))
    params.Set("end", fmt.Sprintf("%d", end.Unix()))
    
    seriesURL := fmt.Sprintf("%s/api/v1/series?%s", vm.baseURL, params.Encode())
    
    resp, err := vm.client.Get(seriesURL)
    if err != nil {
        return nil, fmt.Errorf("failed to fetch series: %w", err)
    }
    defer resp.Body.Close()
    
    if resp.StatusCode != http.StatusOK {
        body, _ := io.ReadAll(resp.Body)
        return nil, fmt.Errorf("series request failed with status %d: %s", resp.StatusCode, string(body))
    }
    
    var seriesResp VMSeriesResponse
    if err := json.NewDecoder(resp.Body).Decode(&seriesResp); err != nil {
        return nil, fmt.Errorf("failed to decode series response: %w", err)
    }
    
    return seriesResp.Data, nil
}

// QuerySeries fetches data for a specific series with automatic chunking for large time ranges
func (vm *VMClient) QuerySeries(metric map[string]string, start, end time.Time, maxPointsPerQuery int) ([]DataPoint, error) {
    // Calculate the total duration and expected points
    duration := end.Sub(start)
    stepSeconds := int64(60) // 1 minute step
    expectedPoints := int(duration.Seconds() / float64(stepSeconds))
    
    // If expected points are within limit, do a single query
    if expectedPoints <= maxPointsPerQuery {
        return vm.querySingleRange(metric, start, end, stepSeconds)
    }
    
    // Otherwise, chunk the query
    allPoints := make([]DataPoint, 0)
    
    // Calculate chunk duration based on max points
    chunkDuration := time.Duration(maxPointsPerQuery*int(stepSeconds)) * time.Second
    
    currentStart := start
    for currentStart.Before(end) {
        currentEnd := currentStart.Add(chunkDuration)
        if currentEnd.After(end) {
            currentEnd = end
        }
        
        points, err := vm.querySingleRange(metric, currentStart, currentEnd, stepSeconds)
        if err != nil {
            return nil, fmt.Errorf("failed to query chunk [%s - %s]: %w", 
                currentStart.Format(time.RFC3339), currentEnd.Format(time.RFC3339), err)
        }
        
        allPoints = append(allPoints, points...)
        currentStart = currentEnd
    }
    
    // Remove any duplicate points at chunk boundaries
    allPoints = removeDuplicatePoints(allPoints)
    
    return allPoints, nil
}

// querySingleRange performs a single query for a time range
func (vm *VMClient) querySingleRange(metric map[string]string, start, end time.Time, stepSeconds int64) ([]DataPoint, error) {
    // Build the query selector
    selector := vm.buildSelector(metric)
    
    params := url.Values{}
    params.Set("query", selector)
    params.Set("start", fmt.Sprintf("%d", start.Unix()))
    params.Set("end", fmt.Sprintf("%d", end.Unix()))
    params.Set("step", fmt.Sprintf("%d", stepSeconds))
    
    queryURL := fmt.Sprintf("%s/api/v1/query_range?%s", vm.baseURL, params.Encode())
    
    resp, err := vm.client.Get(queryURL)
    if err != nil {
        return nil, fmt.Errorf("failed to query series: %w", err)
    }
    defer resp.Body.Close()
    
    if resp.StatusCode != http.StatusOK {
        body, _ := io.ReadAll(resp.Body)
        return nil, fmt.Errorf("query failed with status %d: %s", resp.StatusCode, string(body))
    }
    
    var vmResp VMResponse
    if err := json.NewDecoder(resp.Body).Decode(&vmResp); err != nil {
        return nil, fmt.Errorf("failed to decode response: %w", err)
    }
    
    if len(vmResp.Data.Result) == 0 {
        return nil, nil
    }
    
    // Convert to DataPoints
    result := vmResp.Data.Result[0]
    points := make([]DataPoint, 0, len(result.Values))
    
    for _, val := range result.Values {
        if len(val) != 2 {
            continue
        }
        
        timestamp, ok := val[0].(float64)
        if !ok {
            continue
        }
        
        valueStr, ok := val[1].(string)
        if !ok {
            continue
        }
        
        value, err := strconv.ParseFloat(valueStr, 64)
        if err != nil {
            continue
        }
        
        points = append(points, DataPoint{
            Timestamp: int64(timestamp),
            Value:     value,
        })
    }
    
    return points, nil
}

// removeDuplicatePoints removes duplicate points based on timestamp
func removeDuplicatePoints(points []DataPoint) []DataPoint {
    if len(points) <= 1 {
        return points
    }
    
    // Sort by timestamp
    sort.Slice(points, func(i, j int) bool {
        return points[i].Timestamp < points[j].Timestamp
    })
    
    // Remove duplicates
    result := make([]DataPoint, 0, len(points))
    result = append(result, points[0])
    
    for i := 1; i < len(points); i++ {
        if points[i].Timestamp != points[i-1].Timestamp {
            result = append(result, points[i])
        }
    }
    
    return result
}

// WriteSeries writes summarized data back to VictoriaMetrics
func (vm *VMClient) WriteSeries(series TimeSeries) error {
    // VictoriaMetrics prefers the prometheus format for imports
    var lines []string
    
    metricName := series.Metric["__name__"]
    if metricName == "" {
        metricName = "unnamed_metric"
    }
    
    // Build label string (excluding __name__)
    var labels []string
    for k, v := range series.Metric {
        if k != "__name__" {
            // Properly escape label values
            v = strings.ReplaceAll(v, `\`, `\\`)
            v = strings.ReplaceAll(v, `"`, `\"`)
            v = strings.ReplaceAll(v, "\n", `\n`)
            labels = append(labels, fmt.Sprintf(`%s="%s"`, k, v))
        }
    }
    
    labelStr := ""
    if len(labels) > 0 {
        sort.Strings(labels) // Sort for consistency
        labelStr = "{" + strings.Join(labels, ",") + "}"
    }
    
    // Create import lines in Prometheus format
    for _, point := range series.DataPoints {
        line := fmt.Sprintf("%s%s %g %d", metricName, labelStr, point.Value, point.Timestamp*1000) // VM expects milliseconds
        lines = append(lines, line)
    }
    
    // Join all lines
    data := strings.Join(lines, "\n")
    
    // Send to import/prometheus endpoint
    importURL := fmt.Sprintf("%s/api/v1/import/prometheus", vm.baseURL)
    
    resp, err := vm.client.Post(importURL, "text/plain", strings.NewReader(data))
    if err != nil {
        return fmt.Errorf("failed to write series: %w", err)
    }
    defer resp.Body.Close()
    
    if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
        body, _ := io.ReadAll(resp.Body)
        return fmt.Errorf("import failed with status %d: %s", resp.StatusCode, string(body))
    }
    
    return nil
}

// DeleteSeries deletes original data in chunks to avoid timeout
func (vm *VMClient) DeleteSeries(metric map[string]string, start, end time.Time) error {
    // Delete in monthly chunks to avoid timeouts
    currentStart := start
    
    for currentStart.Before(end) {
        // Calculate chunk end (1 month or remaining time)
        currentEnd := currentStart.AddDate(0, 1, 0)
        if currentEnd.After(end) {
            currentEnd = end
        }
        
        selector := vm.buildSelector(metric)
        
        params := url.Values{}
        params.Set("match[]", selector)
        params.Set("start", fmt.Sprintf("%d", currentStart.Unix()))
        params.Set("end", fmt.Sprintf("%d", currentEnd.Unix()))
        
        deleteURL := fmt.Sprintf("%s/api/v1/admin/tsdb/delete_series?%s", vm.baseURL, params.Encode())
        
        req, err := http.NewRequest("POST", deleteURL, nil)
        if err != nil {
            return fmt.Errorf("failed to create delete request: %w", err)
        }
        
        resp, err := vm.client.Do(req)
        if err != nil {
            return fmt.Errorf("failed to delete series: %w", err)
        }
        resp.Body.Close()
        
        if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
            body, _ := io.ReadAll(resp.Body)
            return fmt.Errorf("delete failed with status %d: %s", resp.StatusCode, string(body))
        }
        
        currentStart = currentEnd
    }
    
    return nil
}

// buildSelector creates a PromQL selector from metric labels
func (vm *VMClient) buildSelector(metric map[string]string) string {
    metricName := metric["__name__"]
    if metricName == "" {
        metricName = ".*"
    }
    
    labels := make([]string, 0)
    for k, v := range metric {
        if k != "__name__" {
            // Escape special characters in label values
            v = strings.ReplaceAll(v, `\`, `\\`)
            v = strings.ReplaceAll(v, `"`, `\"`)
            labels = append(labels, fmt.Sprintf(`%s="%s"`, k, v))
        }
    }
    
    if len(labels) > 0 {
        return fmt.Sprintf(`%s{%s}`, metricName, strings.Join(labels, ","))
    }
    return metricName
}

// LTTB implements the Largest Triangle Three Buckets algorithm
func LTTB(data []DataPoint, threshold int) []DataPoint {
    if len(data) <= threshold || threshold < 3 {
        return data
    }
    
    sampled := make([]DataPoint, 0, threshold)
    
    // Always keep first point
    sampled = append(sampled, data[0])
    
    bucketSize := float64(len(data)-2) / float64(threshold-2)
    
    bucketStart := 1
    
    for i := 0; i < threshold-2; i++ {
        bucketEnd := int(math.Floor(float64(i+1)*bucketSize)) + 1
        if bucketEnd >= len(data)-1 {
            bucketEnd = len(data) - 2
        }
        
        // Calculate average of next bucket
        avgTimestamp := float64(0)
        avgValue := float64(0)
        avgRangeStart := bucketEnd + 1
        avgRangeEnd := int(math.Floor(float64(i+2)*bucketSize)) + 1
        
        if avgRangeEnd >= len(data) {
            avgRangeEnd = len(data)
        }
        
        avgRangeLength := avgRangeEnd - avgRangeStart
        for j := avgRangeStart; j < avgRangeEnd; j++ {
            avgTimestamp += float64(data[j].Timestamp)
            avgValue += data[j].Value
        }
        avgTimestamp /= float64(avgRangeLength)
        avgValue /= float64(avgRangeLength)
        
        // Find point with largest triangle area
        maxArea := float64(-1)
        maxAreaPoint := data[bucketStart]
        
        for j := bucketStart; j <= bucketEnd; j++ {
            area := math.Abs(
                (float64(sampled[len(sampled)-1].Timestamp)-avgTimestamp)*
                    (data[j].Value-sampled[len(sampled)-1].Value)-
                    (float64(sampled[len(sampled)-1].Timestamp)-float64(data[j].Timestamp))*
                    (avgValue-sampled[len(sampled)-1].Value),
            ) * 0.5
            
            if area > maxArea {
                maxArea = area
                maxAreaPoint = data[j]
            }
        }
        
        sampled = append(sampled, maxAreaPoint)
        bucketStart = bucketEnd + 1
    }
    
    // Always keep last point
    sampled = append(sampled, data[len(data)-1])
    
    return sampled
}

// SummarizeByTimeframe applies LTTB to each timeframe
func SummarizeByTimeframe(data []DataPoint, config *Config) []DataPoint {
    if len(data) == 0 {
        return data
    }
    
    // Group data by timeframe
    var groups [][]DataPoint
    
    // Handle custom duration frames (5m, 15m, etc.)
    if config.FrameDuration > 0 {
        groups = groupByDuration(data, config.FrameDuration, config.StartTime, config.EndTime)
    } else {
        // Handle legacy frames (hour, day, month)
        groups = groupByTimeframe(data, config.SummarizationFrame, config.StartTime, config.EndTime)
    }
    
    // Apply LTTB to each group
    result := make([]DataPoint, 0)
    
    for _, group := range groups {
        if len(group) > 0 {
            summarized := LTTB(group, config.BucketsPerFrame)
            result = append(result, summarized...)
        }
    }
    
    // Sort by timestamp
    sort.Slice(result, func(i, j int) bool {
        return result[i].Timestamp < result[j].Timestamp
    })
    
    return result
}

// groupByDuration groups data points by a custom duration
func groupByDuration(data []DataPoint, duration time.Duration, start, end time.Time) [][]DataPoint {
    groups := make([][]DataPoint, 0)
    
    // Align start time to the duration
    currentTime := start.Truncate(duration)
    if currentTime.Before(start) {
        currentTime = currentTime.Add(duration)
    }
    
    for currentTime.Before(end) {
        nextTime := currentTime.Add(duration)
        group := extractTimeRange(data, currentTime.Unix(), nextTime.Unix())
        if len(group) > 0 {
            groups = append(groups, group)
        }
        currentTime = nextTime
    }
    
    return groups
}

// groupByTimeframe groups data points by the specified timeframe (legacy function for hour, day, month)
func groupByTimeframe(data []DataPoint, frame string, start, end time.Time) [][]DataPoint {
    groups := make([][]DataPoint, 0)
    
    switch frame {
    case "hour":
        // Create hourly groups
        currentTime := start.Truncate(time.Hour)
        for currentTime.Before(end) {
            nextTime := currentTime.Add(time.Hour)
            group := extractTimeRange(data, currentTime.Unix(), nextTime.Unix())
            if len(group) > 0 {
                groups = append(groups, group)
            }
            currentTime = nextTime
        }
        
    case "day":
        // Create daily groups
        currentTime := start.Truncate(24 * time.Hour)
        for currentTime.Before(end) {
            nextTime := currentTime.AddDate(0, 0, 1)
            group := extractTimeRange(data, currentTime.Unix(), nextTime.Unix())
            if len(group) > 0 {
                groups = append(groups, group)
            }
            currentTime = nextTime
        }
        
    case "month":
        // Create monthly groups
        currentTime := time.Date(start.Year(), start.Month(), 1, 0, 0, 0, 0, start.Location())
        for currentTime.Before(end) {
            nextTime := currentTime.AddDate(0, 1, 0)
            group := extractTimeRange(data, currentTime.Unix(), nextTime.Unix())
            if len(group) > 0 {
                groups = append(groups, group)
            }
            currentTime = nextTime
        }
    }
    
    return groups
}

// extractTimeRange extracts points within a time range
func extractTimeRange(data []DataPoint, startTime, endTime int64) []DataPoint {
    result := make([]DataPoint, 0)
    for _, point := range data {
        if point.Timestamp >= startTime && point.Timestamp < endTime {
            result = append(result, point)
        }
    }
    return result
}

// parseTimeframe parses the timeframe string and returns appropriate values
func parseTimeframe(frame string) (time.Duration, error) {
    // Try to parse as duration first (5m, 15m, 1h, etc.)
    if duration, err := time.ParseDuration(frame); err == nil {
        return duration, nil
    }
    
    // Handle legacy frames
    switch frame {
    case "hour":
        return time.Hour, nil
    case "day":
        return 24 * time.Hour, nil
    case "month":
        return 0, nil // Special case, handled differently
    default:
        return 0, fmt.Errorf("invalid timeframe: %s. Use formats like '5m', '15m', '1h', 'hour', 'day', or 'month'", frame)
    }
}

// ProcessSeries handles the complete summarization process for one series
func ProcessSeries(vm *VMClient, config *Config, metric map[string]string) (int64, int64, error) {
    // Query original data with chunking support
    data, err := vm.QuerySeries(metric, config.StartTime, config.EndTime, config.MaxPointsPerQuery)
    if err != nil {
        return 0, 0, fmt.Errorf("failed to query data: %w", err)
    }
    
    if len(data) == 0 {
        return 0, 0, nil
    }
    
    originalCount := int64(len(data))
    
    // Apply summarization
    summarized := SummarizeByTimeframe(data, config)
    
    summarizedCount := int64(len(summarized))
    
    if summarizedCount == 0 {
        return originalCount, 0, nil
    }
    
    // Create new metric with either prefix or suffix label
    summarizedMetric := make(map[string]string)
    for k, v := range metric {
        summarizedMetric[k] = v
    }
    
    // Apply prefix or suffix based on configuration
    if config.UsePrefix && config.MetricPrefix != "" {
        // Change the metric name by adding prefix
        originalName := metric["__name__"]
        if originalName != "" {
            summarizedMetric["__name__"] = config.MetricPrefix + originalName
        }
        // NO additional labels when using prefix - the prefix itself indicates summarization
    } else {
        // Use suffix label approach (original behavior)
        summarizedMetric[config.SuffixLabel] = fmt.Sprintf("lttb_%s", config.SummarizationFrame)
    }
    
    // Create time series object
    series := TimeSeries{
        Metric:     summarizedMetric,
        DataPoints: summarized,
    }
    
    if !config.DryRun {
        // Write summarized data
        if err := vm.WriteSeries(series); err != nil {
            return originalCount, summarizedCount, fmt.Errorf("failed to write summarized data: %w", err)
        }
        
        // Delete original data if requested and not using prefix
        // When using prefix, we assume user wants to keep original
        if config.DeleteOriginal && !config.UsePrefix {
            if err := vm.DeleteSeries(metric, config.StartTime, config.EndTime); err != nil {
                return originalCount, summarizedCount, fmt.Errorf("failed to delete original data: %w", err)
            }
        }
    }
    
    return originalCount, summarizedCount, nil
}

// EstimateSpaceSaved calculates approximate space saved
func EstimateSpaceSaved(originalPoints, summarizedPoints int64) int64 {
    // Approximate: each point takes ~16 bytes (8 bytes timestamp + 8 bytes value)
    // In reality, VM uses compression, but this gives a rough estimate
    bytesPerPoint := int64(16)
    return (originalPoints - summarizedPoints) * bytesPerPoint
}

// FormatBytes formats bytes in human-readable format
func FormatBytes(bytes int64) string {
    const unit = 1024
    if bytes < unit {
        return fmt.Sprintf("%d B", bytes)
    }
    div, exp := int64(unit), 0
    for n := bytes / unit; n >= unit; n /= unit {
        div *= unit
        exp++
    }
    return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// PrintSummary displays the final statistics
func PrintSummary(summary *Summary, config *Config) {
    fmt.Println("\n=== SUMMARIZATION COMPLETE ===")
    fmt.Printf("Processing Time:      %v\n", summary.ProcessingTime)
    fmt.Printf("Total Series Found:   %d\n", summary.TotalSeries)
    fmt.Printf("Series Processed:     %d\n", summary.ProcessedSeries)
    fmt.Printf("Series Failed:        %d\n", summary.FailedSeries)
    fmt.Printf("Original Points:      %s\n", formatNumber(summary.OriginalPoints))
    fmt.Printf("Summarized Points:    %s\n", formatNumber(summary.SummarizedPoints))
    
    if summary.OriginalPoints > 0 {
        reduction := float64(summary.OriginalPoints-summary.SummarizedPoints) / float64(summary.OriginalPoints) * 100
        compression := float64(summary.OriginalPoints) / float64(summary.SummarizedPoints)
        fmt.Printf("Data Reduction:       %.1f%%\n", reduction)
        fmt.Printf("Compression Ratio:    %.1fx\n", compression)
    }
    
    fmt.Printf("Estimated Space Saved: %s\n", FormatBytes(summary.EstimatedSpaceSaved))
    
    if config.UsePrefix {
        fmt.Printf("\nNew metrics created with prefix: %s\n", config.MetricPrefix)
        fmt.Println("Original metrics were preserved")
    } else if config.DeleteOriginal {
        fmt.Println("\nOriginal metrics were deleted")
    } else {
        fmt.Println("\nOriginal metrics were preserved")
    }
    
    fmt.Println("================================")
}

// formatNumber formats large numbers with commas
func formatNumber(n int64) string {
    str := fmt.Sprintf("%d", n)
    var result []string
    
    for i := len(str); i > 0; i -= 3 {
        start := i - 3
        if start < 0 {
            start = 0
        }
        result = append([]string{str[start:i]}, result...)
    }
    
    return strings.Join(result, ",")
}

func main() {
    // Parse command line arguments
    var (
        vmURL       = flag.String("vm-url", "http://localhost:8428", "VictoriaMetrics base URL")
        startStr    = flag.String("start", "", "Start date (RFC3339 format, e.g., 2024-01-01T00:00:00Z)")
        endStr      = flag.String("end", "", "End date (RFC3339 format)")
        frame       = flag.String("frame", "hour", "Summarization timeframe: 5m, 15m, 30m, hour, day, month, or any valid duration")
        buckets     = flag.Int("buckets", 100, "Number of LTTB buckets per timeframe")
        batchSize   = flag.Int("batch", 10, "Number of series to process in parallel")
        dryRun      = flag.Bool("dry-run", false, "Show what would be done without making changes")
        deleteOrig  = flag.Bool("delete-original", false, "Delete original data after summarization")
        suffixLabel = flag.String("suffix-label", "summarized", "Label to add to summarized metrics")
        filter      = flag.String("filter", "", "Metric filter (PromQL selector, e.g., 'cpu_usage', '{job=\"node\"}', 'cpu_.*')")
        maxPoints   = flag.Int("max-points", 25000, "Maximum points per query (must be less than VM's limit)")
        metricPrefix = flag.String("metric-prefix", "", "Prefix for new metric names (e.g., 'downsampled_'). When set, original metrics are preserved")
    )
    
    flag.Parse()
    
    // Validate arguments
    if *startStr == "" || *endStr == "" {
        log.Fatal("Both -start and -end dates are required")
    }
    
    startTime, err := time.Parse(time.RFC3339, *startStr)
    if err != nil {
        log.Fatalf("Invalid start date format: %v", err)
    }
    
    endTime, err := time.Parse(time.RFC3339, *endStr)
    if err != nil {
        log.Fatalf("Invalid end date format: %v", err)
    }
    
    if !startTime.Before(endTime) {
        log.Fatal("Start date must be before end date")
    }
    
    // Parse timeframe
    frameDuration, err := parseTimeframe(*frame)
    if err != nil {
        log.Fatalf("Invalid frame: %v", err)
    }
    
    // Determine if using prefix mode
    usePrefix := *metricPrefix != ""
    
    // If using prefix, don't delete original by default
    if usePrefix && *deleteOrig {
        fmt.Println("Warning: When using -metric-prefix, original metrics are preserved by default.")
        fmt.Println("The -delete-original flag will be ignored.")
        *deleteOrig = false
    }
    
    // Create configuration
    config := &Config{
        VMBaseURL:          *vmURL,
        StartTime:          startTime,
        EndTime:            endTime,
        SummarizationFrame: *frame,
        FrameDuration:      frameDuration,
        BucketsPerFrame:    *buckets,
        BatchSize:          *batchSize,
        DryRun:             *dryRun,
        DeleteOriginal:     *deleteOrig,
        SuffixLabel:        *suffixLabel,
        MetricFilter:       *filter,
        MaxPointsPerQuery:  *maxPoints,
        MetricPrefix:       *metricPrefix,
        UsePrefix:          usePrefix,
    }
    
    // Create VM client
    vmClient := NewVMClient(config.VMBaseURL)
    
    fmt.Println("=== VictoriaMetrics LTTB Summarization ===")
    fmt.Printf("VM URL:          %s\n", config.VMBaseURL)
    fmt.Printf("Time Range:      %s to %s\n", startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))
    fmt.Printf("Frame:           %s\n", config.SummarizationFrame)
    fmt.Printf("Buckets/Frame:   %d\n", config.BucketsPerFrame)
    fmt.Printf("Max Points/Query: %d\n", config.MaxPointsPerQuery)
    
    if config.UsePrefix {
        fmt.Printf("Metric Prefix:   %s\n", config.MetricPrefix)
        fmt.Printf("Original Metrics: PRESERVED\n")
    } else {
        fmt.Printf("Suffix Label:    %s\n", config.SuffixLabel)
        fmt.Printf("Delete Original: %v\n", config.DeleteOriginal)
    }
    
    fmt.Printf("Dry Run:         %v\n", config.DryRun)
    if config.MetricFilter != "" {
        fmt.Printf("Filter:          %s\n", config.MetricFilter)
    }
    fmt.Println("==========================================")
    
    startProcessing := time.Now()
    
    // Get all series
    log.Println("Fetching series list...")
    allSeries, err := vmClient.ListAllSeries(config.StartTime, config.EndTime, config.MetricFilter)
    if err != nil {
        log.Fatalf("Failed to list series: %v", err)
    }
    
    fmt.Printf("\nFound %d time series to process\n", len(allSeries))
    
    // Initialize summary
    summary := &Summary{
        TotalSeries: len(allSeries),
    }
    
    // Create a mutex for thread-safe summary updates
    var summaryMu sync.Mutex
    
    // Process series in batches
    var wg sync.WaitGroup
    semaphore := make(chan struct{}, config.BatchSize)
    
    for i, series := range allSeries {
        wg.Add(1)
        go func(idx int, metric map[string]string) {
            defer wg.Done()
            
            // Acquire semaphore
            semaphore <- struct{}{}
            defer func() { <-semaphore }()
            
            // Progress indicator
            if idx%10 == 0 {
                fmt.Printf("\rProcessing series %d/%d...", idx+1, len(allSeries))
            }
            
            original, summarized, err := ProcessSeries(vmClient, config, metric)
            
            // Update summary with mutex
            summaryMu.Lock()
            if err != nil {
                log.Printf("\nError processing series %v: %v", metric, err)
                summary.FailedSeries++
            } else {
                summary.ProcessedSeries++
                summary.OriginalPoints += original
                summary.SummarizedPoints += summarized
            }
            summaryMu.Unlock()
        }(i, series)
    }
    
    // Wait for all goroutines to complete
    wg.Wait()
    
    fmt.Println() // New line after progress
    
    // Calculate final statistics
    summary.ProcessingTime = time.Since(startProcessing)
    summary.EstimatedSpaceSaved = EstimateSpaceSaved(summary.OriginalPoints, summary.SummarizedPoints)
    
    // Print summary
    PrintSummary(summary, config)
    
    if config.DryRun {
        fmt.Println("\nDRY RUN - No changes were made to the database")
    }
}

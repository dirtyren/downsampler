# VictoriaMetrics LTTB Downsampler

A high-performance Go tool for downsampling time series data in VictoriaMetrics using the LTTB (Largest Triangle Three Buckets) algorithm. This tool helps reduce storage costs while preserving the visual characteristics of your metrics data.

## Features

- **LTTB Algorithm**: Preserves the shape and important features of time series data while significantly reducing data points
- **Flexible Timeframes**: Support for custom durations (5m, 15m, 1h) and standard periods (hour, day, month)
- **Smart Chunking**: Automatically handles large time ranges without hitting VictoriaMetrics query limits
- **Cardinality Control**: Option to use metric prefixes instead of labels to avoid cardinality explosion
- **Parallel Processing**: Processes multiple series concurrently for better performance
- **Safe Operations**: Dry-run mode, progress tracking, and detailed statistics
- **Memory Efficient**: Handles millions of data points without excessive memory usage

## What is LTTB?

The Largest Triangle Three Buckets (LTTB) algorithm is a time series downsampling method that:
- Preserves visual similarity to the original data
- Maintains important peaks, valleys, and trends
- Provides deterministic results
- Runs in linear time O(n)

Unlike simple averaging, LTTB ensures that important spikes and anomalies are preserved in the downsampled data, making it ideal for monitoring and visualization use cases.

## Installation

### From Source

```bash
# Clone the repository
git clone https://github.com/yourusername/vm-lttb-downsampler.git
cd vm-lttb-downsampler

# Build the binary
go build -o vm-lttb-downsampler main.go

# Make it executable
chmod +x vm-lttb-downsampler
```

### Using Go Install

```bash
go install github.com/yourusername/vm-lttb-downsampler@latest
```

## Usage

### Basic Usage

```bash
./vm-lttb-downsampler \
  -vm-url "http://localhost:8428" \
  -start "2024-01-01T00:00:00Z" \
  -end "2024-01-31T00:00:00Z" \
  -frame "day" \
  -metric-prefix "daily_"
```

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `-vm-url` | `http://localhost:8428` | VictoriaMetrics base URL |
| `-start` | *required* | Start date in RFC3339 format (e.g., `2024-01-01T00:00:00Z`) |
| `-end` | *required* | End date in RFC3339 format |
| `-frame` | `hour` | Summarization timeframe: `5m`, `15m`, `30m`, `hour`, `day`, `month`, or any valid Go duration |
| `-buckets` | `100` | Number of LTTB buckets per timeframe. Higher = more detail preserved |
| `-metric-prefix` | `""` | Prefix for new metric names (e.g., `downsampled_`). When set, original metrics are preserved |
| `-filter` | `""` | Metric filter - can be metric name, regex pattern, or PromQL selector |
| `-batch` | `10` | Number of series to process in parallel |
| `-max-points` | `25000` | Maximum points per query (must be less than VM's limit) |
| `-delete-original` | `false` | Delete original data after summarization (ignored when using prefix) |
| `-suffix-label` | `summarized` | Label to add to summarized metrics (when not using prefix) |
| `-dry-run` | `false` | Show what would be done without making changes |

## Examples

### 1. Downsample Last Week to Hourly Resolution

```bash
./vm-lttb-downsampler \
  -vm-url "http://localhost:8428" \
  -start "2024-01-01T00:00:00Z" \
  -end "2024-01-08T00:00:00Z" \
  -frame "hour" \
  -buckets 60 \
  -metric-prefix "hourly_"
```

### 2. Create 5-Minute Summaries for Specific Metrics

```bash
./vm-lttb-downsampler \
  -vm-url "http://localhost:8428" \
  -start "2024-01-01T00:00:00Z" \
  -end "2024-01-02T00:00:00Z" \
  -frame "5m" \
  -filter "cpu_usage" \
  -buckets 30 \
  -metric-prefix "5min_"
```

### 3. Downsample Windows Metrics with Pattern Matching

```bash
./vm-lttb-downsampler \
  -vm-url "http://localhost:8428" \
  -start "2023-01-01T00:00:00Z" \
  -end "2024-01-01T00:00:00Z" \
  -frame "day" \
  -filter "windows_.*" \
  -metric-prefix "daily_" \
  -buckets 50
```

### 4. Create Monthly Summaries and Delete Originals

```bash
./vm-lttb-downsampler \
  -vm-url "http://localhost:8428" \
  -start "2023-01-01T00:00:00Z" \
  -end "2024-01-01T00:00:00Z" \
  -frame "month" \
  -delete-original \
  -suffix-label "monthly_summary"
```

### 5. Dry Run to Preview Changes

```bash
./vm-lttb-downsampler \
  -vm-url "http://localhost:8428" \
  -start "2024-01-01T00:00:00Z" \
  -end "2024-01-31T00:00:00Z" \
  -frame "day" \
  -metric-prefix "test_" \
  -dry-run
```

### 6. Process Metrics from Specific Job

```bash
./vm-lttb-downsampler \
  -vm-url "http://localhost:8428" \
  -start "2024-01-01T00:00:00Z" \
  -end "2024-01-31T00:00:00Z" \
  -frame "hour" \
  -filter '{job="node_exporter"}' \
  -metric-prefix "hourly_"
```

## Multi-Level Downsampling Strategy

For optimal storage and query performance, consider implementing multiple downsampling levels:

```bash
# Recent data: 5-minute summaries
./vm-lttb-downsampler \
  -start "$(date -d '7 days ago' --iso-8601=seconds)" \
  -end "$(date --iso-8601=seconds)" \
  -frame "5m" \
  -metric-prefix "5min_"

# Last month: Hourly summaries  
./vm-lttb-downsampler \
  -start "$(date -d '30 days ago' --iso-8601=seconds)" \
  -end "$(date -d '7 days ago' --iso-8601=seconds)" \
  -frame "hour" \
  -metric-prefix "hourly_"

# Older data: Daily summaries
./vm-lttb-downsampler \
  -start "$(date -d '1 year ago' --iso-8601=seconds)" \
  -end "$(date -d '30 days ago' --iso-8601=seconds)" \
  -frame "day" \
  -metric-prefix "daily_"
```

## Querying Downsampled Data

After downsampling, you can query your data at different resolutions:

```promql
# Original data (full resolution)
temperature_celsius{sensor="outdoor"}

# 5-minute downsampled
5min_temperature_celsius{sensor="outdoor"}

# Hourly downsampled
hourly_temperature_celsius{sensor="outdoor"}

# Compare all resolutions
temperature_celsius{sensor="outdoor"} or 
5min_temperature_celsius{sensor="outdoor"} or
hourly_temperature_celsius{sensor="outdoor"}
```

## Output Example

```
=== VictoriaMetrics LTTB Summarization ===
VM URL:          http://localhost:8428
Time Range:      2024-01-01T00:00:00Z to 2024-02-01T00:00:00Z
Frame:           day
Buckets/Frame:   50
Max Points/Query: 25000
Metric Prefix:   daily_
Original Metrics: PRESERVED
Dry Run:         false
==========================================

Fetching series list...
Found 1,247 time series to process

Processing series 1247/1247...

=== SUMMARIZATION COMPLETE ===
Processing Time:      2m34s
Total Series Found:   1,247
Series Processed:     1,245
Series Failed:        2
Original Points:      107,884,320
Summarized Points:    3,118,750
Data Reduction:       97.1%
Compression Ratio:    34.6x
Estimated Space Saved: 1.6 GB

New metrics created with prefix: daily_
Original metrics were preserved
================================
```

## Comparison non downsampled vs downsampled

```bash
 go run vm-lttb-summarizer \
  -vm-url "http://192.168.25.200:8428" \
  -start "2025-08-08T05:00:00Z" \
  -end "2025-09-10T6:00:00Z" \
  -frame "hour" \
  -buckets 30 \
  -metric-prefix "downsampled_"
```

```
=== VictoriaMetrics LTTB Summarization ===
VM URL:          http://192.168.25.200:8428
Time Range:      2025-08-08T05:00:00Z to 2025-09-10T06:00:00Z
Frame:           hour
Buckets/Frame:   30
Max Points/Query: 25000
Metric Prefix:   downsampled_
Original Metrics: PRESERVED
Dry Run:         false
==========================================
2025/08/09 12:32:25 Fetching series list...
2025/08/09 12:32:25 Fetching series list with filter: {__name__=~".+"}
2025/08/09 12:32:25 Found 6487 series matching filter

Found 6487 time series to process
Processing series 6341/6487...

=== SUMMARIZATION COMPLETE ===
Processing Time:      1m7.145451958s
Total Series Found:   6487
Series Processed:     6487
Series Failed:        0
Original Points:      6,348,925
Summarized Points:    3,270,821
Data Reduction:       48.5%
Compression Ratio:    1.9x
Estimated Space Saved: 47.0 MB

New metrics created with prefix: downsampled_
Original metrics were preserved
================================

```

<img width="1787" height="691" alt="Example" src="https://github.com/user-attachments/assets/06dc72ee-7f82-4d88-8d67-cec337428cfa" />


## Performance Tips

1. **Batch Size**: Increase `-batch` for faster processing on systems with more CPU cores
2. **Buckets**: Lower bucket counts = more compression but less detail. Adjust based on your needs
3. **Time Ranges**: Process large time ranges in chunks to avoid memory issues
4. **Filters**: Use specific filters to process only the metrics you need

## Automation

Create a cron job for regular downsampling:

```bash
# /etc/cron.d/vm-downsampler
# Daily downsampling at 2 AM
0 2 * * * user /usr/local/bin/vm-lttb-downsampler -vm-url "http://localhost:8428" -start "$(date -d '24 hours ago' --iso-8601=seconds)" -end "$(date --iso-8601=seconds)" -frame "hour" -metric-prefix "hourly_" >> /var/log/vm-downsampler.log 2>&1
```

## Troubleshooting

### "Too many matching timeseries" Error
- Use the `-filter` parameter to process specific metrics
- The tool automatically handles this by fetching metrics one by one

### "Too many points" Error  
- Reduce `-max-points` parameter
- Use smaller time ranges
- The tool automatically chunks queries to handle this

### Memory Usage
- Process metrics in smaller batches using `-filter`
- Reduce `-batch` parameter for concurrent processing

## License

MIT License - see LICENSE file for details

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

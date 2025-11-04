# MongoDB Atlas Metadata Collector - AI Coding Assistant Instructions

## Project Overview

This project collects comprehensive metadata and performance metrics from MongoDB Atlas clusters across an organization. The codebase focuses on efficient API interactions with Atlas REST APIs and metrics aggregation.

## Core Architecture

### Primary Scripts
- **`atlas_metadata_collector.py`** - Organization-wide metadata collection with performance metrics
- **`cluster_check.py`** - Single project cluster inspection with full metrics

### Key Components
- **`AtlasAPIClient`** - HTTP Digest Auth wrapper for Atlas v1.0 and v2 APIs
- **`AtlasMetadataCollector`** - Main orchestrator for data collection and processing
- **Dual API versioning** - Uses v1.0 for clusters/processes, v2 with date-versioned headers for disk metrics

## Critical Patterns

### API Authentication
Always use HTTP Digest Auth with `requests.auth.HTTPDigestAuth(public_key, private_key)`. Session persistence is used across multiple API calls.

### Error Handling Convention
Use `raise_on_error=False` for optional metrics endpoints. Core endpoints (projects, clusters) should fail fast on errors.

### Metrics Processing Pattern
```python
# Single metric aggregation
stats = self.calculate_metric_stats_from_single(measurement)

# Multiple metrics summing (e.g., read operations)
read_metrics = [cmd_ops, query_ops, getmore_ops]
stats = self.calculate_metric_stats_from_multiple(read_metrics)
```

### Tier Specification Loading
The project loads MongoDB Atlas tier specifications from `atlas_aws.csv` to calculate usage flags. The CSV has a blank first column header - handle this with `if key.strip() == ''`.

## Development Workflows

### Running Collection Scripts
```bash
# Organization-wide collection
python3 atlas_metadata_collector.py --org-id ORG_ID --public-key KEY --private-key SECRET --output results.csv

# Collection with time filtering (business hours only)
python3 atlas_metadata_collector.py --org-id ORG_ID --public-key KEY --private-key SECRET --time-filter-start 14:00 --time-filter-end 23:59 --output results.csv

# Collection with custom tier limits configuration
python3 atlas_metadata_collector.py --org-id ORG_ID --public-key KEY --private-key SECRET --tier-limits-file custom_limits.csv --output results.csv

# Single project inspection
python3 cluster_check.py  # Uses hardcoded credentials in script
```

### Output Formats
- **JSON** - Hierarchical structure with projects containing clusters array
- **CSV** - Flattened format with 33 columns including usage flags and tier limits

## Data Structure Conventions

### Cluster Metadata Schema
Core fields: `cluster_name`, `cluster_id`, `cluster_type`, `mongodb_version`, `state`, `provider`, `region`, `tier`, `disk_size_gb`

Performance metrics: CPU (max/avg %), Memory (max/avg GB), IOPS, Connections, Read/Write ops per second

Usage flags: `low_cpu_use`, `low_memory_use`, `low_iops_use` (set to `True` or `None` based on <40% threshold)

### API Endpoint Patterns
- **v1.0 Base**: `https://cloud.mongodb.com/api/atlas/v1.0`
- **v2 Disk API**: `https://cloud.mongodb.com/api/atlas/v2` with header `Accept: application/vnd.atlas.2025-11-02+json`

## External Dependencies

### Atlas API Requirements
- Organization-level API key with read permissions
- Project IDs for cluster-specific operations
- Process IDs for metrics collection (auto-discovered from clusters)

### Tier Data Dependency
The `atlas_aws.csv` file contains MongoDB tier specifications (CPU cores, RAM GB, max connections, IOPS limits) used for usage analysis. Keep this synchronized with Atlas pricing.

### Usage Threshold Configuration
The `tier_limits.csv` file defines low-usage thresholds for different metrics. Format: `metric,low_usage_threshold,lower_tier_threshold` with metrics: cpu, memory, iops, connections, disk. The `atlas_aws.csv` includes a `sort` column for tier hierarchy.

## Common Extension Points

### Adding New Metrics
1. Add measurement type to API call in `get_process_measurements()`
2. Add aggregation logic in `collect_cluster_metadata()`
3. Update CSV headers in `save_results()` if using CSV output

### Supporting New Cloud Providers
The current logic handles `providerSettings` from Atlas API. GCP/Azure clusters follow the same pattern as AWS.

### Time-Based Metrics Filtering
Use `--time-filter-start` and `--time-filter-end` parameters to collect metrics only during specific hours of the day. Useful for analyzing usage during business hours or peak traffic periods.

### Custom Usage Thresholds
Configure low-usage thresholds via `tier_limits.csv` file with columns: `metric`, `low_usage_threshold`, `lower_tier_threshold`. Default thresholds: CPU 40%/80%, Memory 40%/80%, IOPS 40%/80%, Connections 80%/80%, Disk 85%/80%.

### Lower Tier Analysis
The system calculates whether workloads could run on the next lower MongoDB Atlas tier. Uses `sort` column in `atlas_aws.csv` to determine tier hierarchy and compares current usage against lower tier limits.

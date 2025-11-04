# MongoDB Atlas Metadata Collector

This repository contains Python scripts to collect and analyze cluster metadata from MongoDB Atlas.

## Scripts

### `atlas_metadata_collector.py`
Collects cluster metadata from MongoDB Atlas for all projects in an organization.

### `cluster_check.py`
Checks clusters in a specific MongoDB Atlas project with full metrics collection (CPU, Memory, IOPS, Disk, Connections, Operations).

## Features

- Collects metadata for all projects in an organization
- For each cluster, gathers:
  - Cluster name, ID, type
  - MongoDB version
  - Provider (AWS, Azure, GCP)
  - Region
  - Tier/Instance size
  - Disk size
  - State
  - Created date
  - Resource usage metrics (CPU, Memory, IOPS, Disk, Connections, Operations)

## Requirements

- Python 3.7+
- MongoDB Atlas API credentials

## Installation

```bash
pip3 install -r requirements.txt
```

## Usage

Both scripts support command-line arguments and environment variables for credentials.

### Environment Variables

You can set credentials using environment variables or a `.env` file:

```bash
export ATLAS_PUBLIC_KEY="your-public-key"
export ATLAS_PRIVATE_KEY="your-private-key"
export ATLAS_ORG_ID="your-org-id"
export ATLAS_PROJECT_ID="your-project-id"
```

### atlas_metadata_collector.py

Collects metadata from all projects in an organization. The script supports both JSON and CSV output formats:

#### JSON Output

```bash
python3 atlas_metadata_collector.py \
  --org-id YOUR_ORG_ID \
  --public-key YOUR_PUBLIC_KEY \
  --private-key YOUR_PRIVATE_KEY \
  --output atlas_metadata.json \
  --pretty
```

#### CSV Output

```bash
python3 atlas_metadata_collector.py \
  --org-id YOUR_ORG_ID \
  --public-key YOUR_PUBLIC_KEY \
  --private-key YOUR_PRIVATE_KEY \
  --output atlas_metadata.csv
```

The output format is automatically detected by the file extension (`.json` or `.csv`).

### cluster_check.py

Checks clusters in a specific project and outputs detailed metrics to `clusters_check.json`:

```bash
python3 cluster_check.py \
  --project-id YOUR_PROJECT_ID \
  --public-key YOUR_PUBLIC_KEY \
  --private-key YOUR_PRIVATE_KEY
```

Or using environment variables:

```bash
python3 cluster_check.py
```

## Usage Flags Calculation

The scripts calculate low usage flags based on tier specifications loaded from `atlas_aws.csv`. The tier limits are matched by joining the cluster's `tier` field from the API data to the `tier` column in the CSV file.

### Flag Calculations

- **`low_iops_use`**: `true` if `iops_avg < 0.75 * iops_tier_limit`
  - Compares average IOPS usage against 75% of compare tier's IOPS limit
  
- **`low_memory_use`**: `true` if `memory_max_gb < memory_tier_limit_gb * 0.75`
  - Compares maximum memory usage against 75% of compare tier's RAM limit
  
- **`low_cpu_use`**: `true` if `cpu_avg_percent < 37`
  - Flags clusters with average CPU usage below 37%
  
- **`low_disk_use`**: `true` if `disk_usage_max_gb < disk_size_gb * 0.3`
  - Flags clusters using less than 30% of their allocated disk space

### Tier Specifications

Tier limits (CPU, RAM, IOPS) are loaded from `atlas_aws.csv`, which contains tier specifications. The CSV must have columns: `tier`, `cpu`, `ram`, `connection`, and `iops`. Clusters with tiers not found in the CSV will have `null` values for tier limits and usage flags.

## Getting MongoDB Atlas Credentials

1. Log in to MongoDB Atlas: https://cloud.mongodb.com/
2. Go to "Access Manager" → "API Keys"
3. Create an API key with read permissions
4. Copy the Public Key and Private Key
5. Get Organization ID from "Settings" → "Organization Settings"
6. Get Project ID from the project's URL or "Settings" → "Project Settings"

## License

This script is provided as-is for collecting MongoDB Atlas metadata. It is not supported by MongoDB, Inc. under any of their commercial support subscriptions or otherwise. Any usage of this script is at your own risk.


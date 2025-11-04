# MongoDB Atlas Metadata Collector

This Python script collects cluster metadata from MongoDB Atlas for all projects in an organization.

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

## Requirements

- Python 3.7+
- MongoDB Atlas API credentials

## Installation

```bash
pip3 install -r requirements.txt
```

## Usage

The script supports both JSON and CSV output formats:

### JSON Output

```bash
python3 atlas_metadata_collector.py \
  --org-id YOUR_ORG_ID \
  --public-key YOUR_PUBLIC_KEY \
  --private-key YOUR_PRIVATE_KEY \
  --output atlas_metadata.json \
  --pretty
```

### CSV Output

```bash
python3 atlas_metadata_collector.py \
  --org-id YOUR_ORG_ID \
  --public-key YOUR_PUBLIC_KEY \
  --private-key YOUR_PRIVATE_KEY \
  --output atlas_metadata.csv
```

The output format is automatically detected by the file extension (`.json` or `.csv`).

## Usage Flags Calculation

The scripts calculate low usage flags based on tier specifications loaded from `atlas_aws.csv`. The tier limits are matched by joining the cluster's `tier` field from the API data to the `tier` column in the CSV file.

### Flag Calculations

- **`low_iops_use`**: `true` if `iops_avg < 0.75 * iops_tier_limit`
  - Compares average IOPS usage against 75% of the tier's IOPS limit
  
- **`low_memory_use`**: `true` if `memory_max_gb < memory_tier_limit_gb * 0.75`
  - Compares maximum memory usage against 75% of the tier's RAM limit
  
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

## License

This script is provided as-is for collecting MongoDB Atlas metadata.


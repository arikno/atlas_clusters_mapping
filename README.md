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

```bash
python3 atlas_metadata_collector.py \
  --org-id YOUR_ORG_ID \
  --public-key YOUR_PUBLIC_KEY \
  --private-key YOUR_PRIVATE_KEY \
  --output atlas_metadata.json \
  --pretty
```

## Getting MongoDB Atlas Credentials

1. Log in to MongoDB Atlas: https://cloud.mongodb.com/
2. Go to "Access Manager" → "API Keys"
3. Create an API key with read permissions
4. Copy the Public Key and Private Key
5. Get Organization ID from "Settings" → "Organization Settings"

## License

This script is provided as-is for collecting MongoDB Atlas metadata.


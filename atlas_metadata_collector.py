#!/usr/bin/env python3
"""
MongoDB Atlas Metadata Collector

This script collects cluster metadata from MongoDB Atlas including:
- Project and cluster information
- Resource usage metrics (when available)
- Cluster tier information

Requirements:
    pip install requests python-dotenv

Usage:
    python atlas_metadata_collector.py --org-id YOUR_ORG_ID --public-key YOUR_PUBLIC_KEY --private-key YOUR_PRIVATE_KEY
"""

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone, time
from typing import Dict, List, Optional, Tuple
import requests
from dotenv import load_dotenv

load_dotenv()


class AtlasAPIClient:
    """Client for interacting with MongoDB Atlas API"""
    
    BASE_URL = "https://cloud.mongodb.com/api/atlas/v1.0"
    
    def __init__(self, public_key: str, private_key: str, org_id: str):
        self.public_key = public_key
        self.private_key = private_key
        self.org_id = org_id
        self.session = requests.Session()
        self.session.auth = requests.auth.HTTPDigestAuth(public_key, private_key)
    
    def _get(self, endpoint: str, params: Optional[Dict] = None, raise_on_error: bool = True) -> Optional[Dict]:
        url = f"{self.BASE_URL}{endpoint}"
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if raise_on_error:
                print(f"HTTP Error for {endpoint}: {e}")
                if hasattr(response, 'text'):
                    print(f"Response: {response.text}")
                raise
            return None
        except requests.exceptions.RequestException as e:
            if raise_on_error:
                print(f"Request Error for {endpoint}: {e}")
                raise
            return None
    
    def get_projects(self) -> List[Dict]:
        print(f"Fetching projects for organization {self.org_id}...")
        response = self._get(f"/orgs/{self.org_id}/groups")
        return response.get("results", [])
    
    def get_clusters(self, project_id: str) -> List[Dict]:
        print(f"  Fetching clusters for project {project_id}...")
        response = self._get(f"/groups/{project_id}/clusters")
        return response.get("results", [])
    
    def get_processes(self, project_id: str) -> List[Dict]:
        """Get all processes for a project"""
        response = self._get(f"/groups/{project_id}/processes", raise_on_error=False)
        if not response:
            return []
        return response.get("results", [])
    
    def get_process_measurements(self, project_id: str, process_id: str, measurement_type: str,
                                granularity: str = "PT1H", period: str = "P7D") -> Optional[Dict]:
        """Get process-level measurements"""
        params = {
            'granularity': granularity,
            'period': period,
            'measurementType': measurement_type,
        }
        endpoint = f"/groups/{project_id}/processes/{process_id}/measurements"
        return self._get(endpoint, params=params, raise_on_error=False)
    
    def get_disks(self, project_id: str, process_id: str) -> List[Dict]:
        """Get all disks for a process using v2 API"""
        endpoint = f"/groups/{project_id}/processes/{process_id}/disks"
        # Use v2 API endpoint
        url = f"https://cloud.mongodb.com/api/atlas/v2{endpoint}"
        try:
            # v2 API requires special Accept header
            headers = {"Accept": "application/vnd.atlas.2025-11-02+json"}
            response = self.session.get(url, headers=headers)
            response.raise_for_status()
            result = response.json()
            return result.get("results", [])
        except requests.exceptions.RequestException:
            return []
    
    def get_disk_measurements(self, project_id: str, process_id: str, partition_name: str,
                             granularity: str = "PT1H", period: str = "P7D") -> Optional[Dict]:
        """Get disk-level measurements using v2 API"""
        params = {
            'granularity': granularity,
            'period': period,
            'measurementTypes': 'DISK_PARTITION_IOPS_TOTAL'
        }
        # Correct endpoint: /disks/{partition}/measurements
        endpoint = f"/groups/{project_id}/processes/{process_id}/disks/{partition_name}/measurements"
        url = f"https://cloud.mongodb.com/api/atlas/v2{endpoint}"
        try:
            headers = {"Accept": "application/vnd.atlas.2025-11-02+json"}
            response = self.session.get(url, params=params, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError:
            return None
        except requests.exceptions.RequestException:
            return None


class AtlasMetadataCollector:
    """Collects comprehensive metadata from MongoDB Atlas"""
    
    def __init__(self, public_key: str, private_key: str, org_id: str, time_filter: Optional[Tuple[str, str]] = None, tier_limits_file: str = "tier_limits.csv"):
        self.client = AtlasAPIClient(public_key, private_key, org_id)
        self.time_filter = time_filter
        self.tier_limits_file = tier_limits_file
    
    def _is_within_time_filter(self, timestamp: str) -> bool:
        """Check if a timestamp is within the configured time filter"""
        if not self.time_filter:
            return True
        
        try:
            # Parse timestamp (ISO format: 2023-10-01T14:30:00Z)
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            # Convert to UTC time
            time_of_day = dt.time()
            
            # Parse start and end times
            start_time = time.fromisoformat(self.time_filter[0])
            end_time = time.fromisoformat(self.time_filter[1])
            
            # Handle time ranges that cross midnight
            if start_time <= end_time:
                return start_time <= time_of_day <= end_time
            else:
                return time_of_day >= start_time or time_of_day <= end_time
        except (ValueError, TypeError):
            # If we can't parse the timestamp, include it
            return True

    def calculate_metric_stats_from_single(self, measurement: Dict) -> Dict[str, float]:
        """Calculate max and avg for a single measurement object"""
        data_points = []
        
        for datapoint in measurement.get("dataPoints", []):
            if datapoint.get("value") is not None:
                # Apply time filter if configured
                if self._is_within_time_filter(datapoint.get("timestamp", "")):
                    data_points.append(datapoint["value"])
        
        if not data_points:
            return {"max": None, "avg": None, "data_point_count": 0}
        
        max_val = max(data_points)
        avg_val = sum(data_points) / len(data_points)
        
        return {"max": round(max_val, 2), "avg": round(avg_val, 2), "data_point_count": len(data_points)}
    
    def calculate_metric_stats_from_multiple(self, measurements: List[Dict]) -> Dict[str, float]:
        """
        Calculate max and avg by summing multiple metrics at each timestamp
        
        Args:
            measurements: List of measurement dictionaries with dataPoints
            
        Returns:
            Dictionary with max, avg, and data_point_count
        """
        # Collect all timestamps that pass the time filter
        all_timestamps = set()
        for measurement in measurements:
            for datapoint in measurement.get("dataPoints", []):
                timestamp = datapoint.get("timestamp")
                if timestamp and self._is_within_time_filter(timestamp):
                    all_timestamps.add(timestamp)
        
        if not all_timestamps:
            return {"max": None, "avg": None, "data_point_count": 0}
        
        # Sum values at each timestamp
        timestamp_sums = {}
        for timestamp in all_timestamps:
            timestamp_sums[timestamp] = 0
            for measurement in measurements:
                for datapoint in measurement.get("dataPoints", []):
                    if datapoint.get("timestamp") == timestamp and datapoint.get("value") is not None:
                        timestamp_sums[timestamp] += datapoint["value"]
        
        sums = list(timestamp_sums.values())
        if not sums:
            return {"max": None, "avg": None, "data_point_count": 0}
        
        max_val = max(sums)
        avg_val = sum(sums) / len(sums)
        
        return {"max": round(max_val, 2), "avg": round(avg_val, 2), "data_point_count": len(sums)}
    
    def load_tier_specs(self) -> Dict:
        """Load tier specifications from CSV file"""
        tier_specs = {}
        try:
            with open('atlas_aws.csv', 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # First column is the tier name (no header name)
                    tier_name = None
                    for key, value in row.items():
                        if key.strip() == '':
                            tier_name = value.strip()
                            break
                    if tier_name:
                        tier_specs[tier_name] = {
                            'cpu': float(row.get('cpu', 0)),
                            'ram': float(row.get('ram', 0)),
                            'connections': float(row.get('connections', 0)),
                            'iops': float(row.get('iops', 0)),
                            'sort': int(row.get('sort', 999))
                        }
        except FileNotFoundError:
            print("Warning: tier specs file not found")
        return tier_specs
    
    def load_tier_limits(self) -> Dict[str, Dict[str, float]]:
        """Load configurable tier limits from CSV file"""
        tier_limits = {
            'cpu': {'low_usage': 40.0, 'lower_tier': 80.0},
            'memory': {'low_usage': 40.0, 'lower_tier': 80.0},
            'iops': {'low_usage': 40.0, 'lower_tier': 80.0},
            'connections': {'low_usage': 80.0, 'lower_tier': 80.0},
            'disk': {'low_usage': 85.0, 'lower_tier': 80.0}
        }
        
        try:
            with open(self.tier_limits_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    metric = row.get('metric', '').strip()
                    low_threshold = row.get('low_usage_threshold', '').strip()
                    lower_tier_threshold = row.get('lower_tier_threshold', '').strip()
                    
                    if metric:
                        if metric not in tier_limits:
                            tier_limits[metric] = {'low_usage': 40.0, 'lower_tier': 80.0}
                        
                        if low_threshold:
                            try:
                                tier_limits[metric]['low_usage'] = float(low_threshold)
                            except ValueError:
                                print(f"Warning: Invalid low_usage_threshold value '{low_threshold}' for metric '{metric}'")
                        
                        if lower_tier_threshold:
                            try:
                                tier_limits[metric]['lower_tier'] = float(lower_tier_threshold)
                            except ValueError:
                                print(f"Warning: Invalid lower_tier_threshold value '{lower_tier_threshold}' for metric '{metric}'")
        except FileNotFoundError:
            print(f"Warning: Tier limits file '{self.tier_limits_file}' not found, using defaults")
        except Exception as e:
            print(f"Warning: Error reading tier limits file: {e}")
        
        return tier_limits

    def find_lower_tier(self, current_tier: str, tier_specs: Dict) -> Optional[Dict]:
        """Find the next lower tier based on sort order"""
        if current_tier not in tier_specs:
            return None
        
        current_sort = tier_specs[current_tier]['sort']
        lower_tier = None
        lower_sort = 0
        
        # Find the tier with the highest sort value that's still lower than current
        for tier_name, spec in tier_specs.items():
            tier_sort = spec['sort']
            if tier_sort < current_sort and tier_sort > lower_sort:
                lower_tier = tier_name
                lower_sort = tier_sort
        
        return tier_specs.get(lower_tier) if lower_tier else None

    def calculate_usage_flags(self, metadata: Dict, tier_specs: Dict) -> Dict:
        """Calculate low usage flags based on tier specifications and configurable limits"""
        tier = metadata.get("tier")
        if not tier or tier not in tier_specs:
            return metadata
        
        spec = tier_specs[tier]
        limits = self.load_tier_limits()
        lower_spec = self.find_lower_tier(tier, tier_specs)
        
        # Calculate memory usage percentage
        memory_avg = metadata.get("memory_avg_gb")
        ram_limit = spec.get("ram")
        if memory_avg is not None and ram_limit:
            memory_usage_percent = (memory_avg / ram_limit) * 100
            metadata["low_memory_use"] = True if memory_usage_percent < limits['memory']['low_usage'] else None
            metadata["memory_tier_limit_gb"] = ram_limit
            
            # Lower tier calculations
            if lower_spec:
                lower_ram_limit = lower_spec.get("ram")
                metadata["memory_lower_tier_limit_gb"] = lower_ram_limit
                if lower_ram_limit:
                    lower_memory_usage_percent = (memory_avg / lower_ram_limit) * 100
                    metadata["memory_lower_tier_acceptable_use"] = True if lower_memory_usage_percent < limits['memory']['lower_tier'] else None
            else:
                metadata["memory_lower_tier_limit_gb"] = None
                metadata["memory_lower_tier_acceptable_use"] = None
        
        # Calculate IOPS usage percentage
        iops_avg = metadata.get("iops_avg")
        iops_limit = spec.get("iops")
        if iops_avg is not None and iops_limit:
            iops_usage_percent = (iops_avg / iops_limit) * 100
            metadata["low_iops_use"] = True if iops_usage_percent < limits['iops']['low_usage'] else None
            metadata["iops_tier_limit"] = iops_limit
            
            # Lower tier calculations
            if lower_spec:
                lower_iops_limit = lower_spec.get("iops")
                metadata["iops_lower_tier_limit"] = lower_iops_limit
                if lower_iops_limit:
                    lower_iops_usage_percent = (iops_avg / lower_iops_limit) * 100
                    metadata["iops_lower_tier_acceptable_use"] = True if lower_iops_usage_percent < limits['iops']['lower_tier'] else None
            else:
                metadata["iops_lower_tier_limit"] = None
                metadata["iops_lower_tier_acceptable_use"] = None
        
        # Calculate CPU usage percentage
        cpu_avg = metadata.get("cpu_avg_percent")
        cpu_limit = spec.get("cpu")
        if cpu_avg is not None:
            metadata["low_cpu_use"] = True if cpu_avg < limits['cpu']['low_usage'] else None
            metadata["cpu_tier_limit"] = cpu_limit
            
            # Lower tier calculations
            if lower_spec:
                lower_cpu_limit = lower_spec.get("cpu")
                metadata["cpu_lower_tier_limit"] = lower_cpu_limit
                if lower_cpu_limit and cpu_limit:
                    # For CPU, we compare against the lower tier's capacity
                    lower_cpu_usage_percent = (cpu_avg / cpu_limit) * 100  # Still use current usage
                    metadata["cpu_lower_tier_acceptable_use"] = True if lower_cpu_usage_percent < limits['cpu']['lower_tier'] else None
            else:
                metadata["cpu_lower_tier_limit"] = None
                metadata["cpu_lower_tier_acceptable_use"] = None

        # Calculate Connections usage percentage
        connections_avg = metadata.get("connections_avg")
        connections_limit = spec.get("connections")
        if connections_avg is not None and connections_limit:
            connections_usage_percent = (connections_avg / connections_limit) * 100
            metadata["low_connections_use"] = True if connections_usage_percent < limits['connections']['low_usage'] else None
            metadata["connections_tier_limit"] = connections_limit
            
            # Lower tier calculations
            if lower_spec:
                lower_connections_limit = lower_spec.get("connections")
                metadata["connections_lower_tier_limit"] = lower_connections_limit
                if lower_connections_limit:
                    lower_connections_usage_percent = (connections_avg / lower_connections_limit) * 100
                    metadata["connections_lower_tier_acceptable_use"] = True if lower_connections_usage_percent < limits['connections']['lower_tier'] else None
            else:
                metadata["connections_lower_tier_limit"] = None
                metadata["connections_lower_tier_acceptable_use"] = None

        # Calculate Disk usage percentage (using disk_usage_max_gb vs disk_size_gb)
        disk_usage_gb = metadata.get("disk_usage_max_gb")
        disk_size_gb = metadata.get("disk_size_gb")
        if disk_usage_gb is not None and disk_size_gb:
            disk_usage_percent = (disk_usage_gb / disk_size_gb) * 100
            metadata["low_disk_use"] = True if disk_usage_percent < limits['disk']['low_usage'] else None
            metadata["disk_tier_limit_gb"] = disk_size_gb
            
            # For disk, lower tier calculation is conceptual - we don't have lower disk sizes
            # So we'll set these to None for now
            metadata["disk_lower_tier_limit_gb"] = None
            metadata["disk_lower_tier_acceptable_use"] = None

        return metadata
    
    def collect_cluster_metadata(self, project_id: str, cluster: Dict) -> Dict:
        """Collect metadata for a single cluster"""
        cluster_name = cluster["name"]
        print(f"    Collecting metadata for cluster: {cluster_name}")
        
        # Start with basic cluster info
        metadata = {
            "cluster_name": cluster_name,
            "cluster_id": cluster.get("id"),
            "cluster_type": cluster.get("clusterType"),
            "mongodb_version": cluster.get("mongoDBVersion"),
            "state": cluster.get("stateName"),
            "created_at": cluster.get("createDate"),
            "updated_at": cluster.get("updateDate"),
        }
        
        # Extract provider settings
        provider_settings = cluster.get("providerSettings", {})
        if provider_settings:
            metadata["provider"] = provider_settings.get("providerName")
            metadata["region"] = provider_settings.get("regionName")
            metadata["tier"] = provider_settings.get("instanceSizeName")
            metadata["disk_size_gb"] = cluster.get("diskSizeGB")
        
        # Extract tier from replicationSpecs if not found
        if not metadata.get("tier"):
            replication_specs = cluster.get("replicationSpecs", [])
            if replication_specs and len(replication_specs) > 0:
                regions_config = replication_specs[0].get("regionsConfig", {})
                if regions_config:
                    first_config = list(regions_config.values())[0]
                    if "electableSpecs" in first_config and len(first_config["electableSpecs"]) > 0:
                        metadata["tier"] = first_config["electableSpecs"][0].get("instanceSize", None)
                    elif "readOnlySpecs" in first_config and len(first_config["readOnlySpecs"]) > 0:
                        metadata["tier"] = first_config["readOnlySpecs"][0].get("instanceSize", None)
                    elif "analyticsSpecs" in first_config and len(first_config["analyticsSpecs"]) > 0:
                        metadata["tier"] = first_config["analyticsSpecs"][0].get("instanceSize", None)
        
        # Get region from replicationSpecs if not already set
        if not metadata.get("region"):
            replication_specs = cluster.get("replicationSpecs", [])
            if replication_specs and len(replication_specs) > 0:
                regions_config = replication_specs[0].get("regionsConfig", {})
                if regions_config:
                    first_region_key = list(regions_config.keys())[0] if regions_config else None
                    if first_region_key:
                        metadata["region"] = first_region_key
        
        # Get disk size if not already set
        if not metadata.get("disk_size_gb"):
            metadata["disk_size_gb"] = cluster.get("diskSizeGB")
        
        # Metrics fields (will be populated if metrics are available)
        metadata.update({
            "cpu_max_percent": None,
            "cpu_avg_percent": None,
            "memory_max_gb": None,
            "memory_avg_gb": None,
            "iops_max": None,
            "iops_avg": None,
            "connections_max": None,
            "connections_avg": None,
            "read_ops_max": None,
            "read_ops_avg": None,
            "write_ops_max": None,
            "write_ops_avg": None,
            "disk_usage_max_gb": None,
            "disk_available_max_gb": None,
            "low_memory_use": None,
            "low_iops_use": None,
            "low_cpu_use": None,
            "low_connections_use": None,
            "low_disk_use": None,
            "cpu_tier_limit": None,
            "memory_tier_limit_gb": None,
            "iops_tier_limit": None,
            "connections_tier_limit": None,
            "disk_tier_limit_gb": None,
            "cpu_lower_tier_limit": None,
            "memory_lower_tier_limit_gb": None,
            "iops_lower_tier_limit": None,
            "connections_lower_tier_limit": None,
            "disk_lower_tier_limit_gb": None,
            "cpu_lower_tier_acceptable_use": None,
            "memory_lower_tier_acceptable_use": None,
            "iops_lower_tier_acceptable_use": None,
            "connections_lower_tier_acceptable_use": None,
            "disk_lower_tier_acceptable_use": None,
        })
        
        # Try to fetch metrics if available
        try:
            print(f"      Attempting to fetch metrics...")
            processes = self.client.get_processes(project_id)
            
            if processes:
                # Match processes to this cluster using mongoURI and userAlias
                cluster_processes = []
                mongo_uri = cluster.get("mongoURI", "")
                
                # Extract hostnames from mongoURI
                uri_hostnames = set()
                if mongo_uri:
                    for uri_part in mongo_uri.split(","):
                        if "://" in uri_part:
                            uri_part = uri_part.split("://")[1]
                        if "/?" in uri_part:
                            uri_part = uri_part.split("/?")[0]
                        if ":" in uri_part:
                            uri_hostname = uri_part.split(":")[0]
                            uri_hostnames.add(uri_hostname)
                
                # Match processes whose hostnames or userAlias appear in the URI
                for p in processes:
                    hostname = p.get("hostname", "")
                    user_alias = p.get("userAlias", "")
                    # Try multiple matching strategies
                    if hostname in uri_hostnames:
                        cluster_processes.append(p)
                        continue
                    if user_alias and user_alias in uri_hostnames:
                        cluster_processes.append(p)
                        continue
                
                # If no matches, use cluster name pattern matching
                if not cluster_processes:
                    cluster_name = cluster.get("name", "")
                    for p in processes:
                        hostname = p.get("hostname", "")
                        if cluster_name.lower().replace("-", "").replace("_", "") in hostname.lower().replace("-", "").replace("_", ""):
                            cluster_processes.append(p)
                
                # Try to find the primary process
                primary_process = None
                for p in cluster_processes:
                    if p.get("typeName") == "REPLICA_PRIMARY":
                        primary_process = p
                        break
                
                if not primary_process and cluster_processes:
                    primary_process = cluster_processes[0]
                
                if not primary_process:
                    # Fallback to any primary in the project
                    for p in processes:
                        if p.get("typeName") == "REPLICA_PRIMARY":
                            primary_process = p
                            break
                
                if not primary_process and processes:
                    primary_process = processes[0]
                
                if not primary_process:
                    print("      No processes found for metrics collection")
                    return metadata
                
                process_id = primary_process["id"]
                process_type = primary_process.get("typeName", "UNKNOWN")
                print(f"      Using process: {primary_process.get('hostname')} ({process_type})")
                
                # Collect CPU metrics - sum multiple metrics
                cpu_measurements = self.client.get_process_measurements(
                    project_id, process_id, "CPU_USAGE", granularity="PT1M", period="P2D"
                )
                if cpu_measurements:
                    cpu_metric_names = [
                        "SYSTEM_NORMALIZED_CPU_GUEST", "SYSTEM_NORMALIZED_CPU_IOWAIT",
                        "SYSTEM_NORMALIZED_CPU_IRQ", "SYSTEM_NORMALIZED_CPU_KERNEL",
                        "SYSTEM_NORMALIZED_CPU_NICE", "SYSTEM_NORMALIZED_CPU_SOFTIRQ",
                        "SYSTEM_NORMALIZED_CPU_STEAL", "SYSTEM_NORMALIZED_CPU_USER"
                    ]
                    cpu_metrics_to_sum = [
                        m for m in cpu_measurements.get("measurements", [])
                        if m.get("name") in cpu_metric_names
                    ]
                    if cpu_metrics_to_sum:
                        stats = self.calculate_metric_stats_from_multiple(cpu_metrics_to_sum)
                        if stats["max"] is not None:
                            metadata["cpu_max_percent"] = stats["max"]
                            metadata["cpu_avg_percent"] = stats["avg"]
                
                # Collect MEMORY metrics
                memory_measurements = self.client.get_process_measurements(
                    project_id, process_id, "MEMORY", granularity="PT1M", period="P2D"
                )
                if memory_measurements:
                    for measurement in memory_measurements.get("measurements", []):
                        metric_name = measurement.get("name")
                        if metric_name == "SYSTEM_MEMORY_USED":
                            stats = self.calculate_metric_stats_from_single(measurement)
                            if stats["max"] is not None:
                                # SYSTEM_MEMORY_USED is in KB, convert to GB
                                metadata["memory_max_gb"] = round(stats["max"] / (1024**2), 2)
                                metadata["memory_avg_gb"] = round(stats["avg"] / (1024**2), 2)
                                break
                
                # Collect DISK and DATABASE_SIZE metrics
                disk_measurements = self.client.get_process_measurements(
                    project_id, process_id, "DISK", granularity="PT1M", period="P2D"
                )
                if disk_measurements:
                    for measurement in disk_measurements.get("measurements", []):
                        metric_name = measurement.get("name")
                        if metric_name == "DB_STORAGE_TOTAL":
                            stats = self.calculate_metric_stats_from_single(measurement)
                            if stats["max"] is not None:
                                # Convert bytes to GB
                                metadata["disk_usage_max_gb"] = round(stats["max"] / (1024**3), 2)
                                break
                
                # Try DATABASE_SIZE for DB_DATA_SIZE_TOTAL
                db_size_measurements = self.client.get_process_measurements(
                    project_id, process_id, "DATABASE_SIZE", granularity="PT1M", period="P2D"
                )
                if db_size_measurements:
                    for measurement in db_size_measurements.get("measurements", []):
                        metric_name = measurement.get("name")
                        if metric_name == "DB_DATA_SIZE_TOTAL":
                            stats = self.calculate_metric_stats_from_single(measurement)
                            if stats["max"] is not None:
                                # Convert bytes to GB
                                if metadata.get("disk_usage_max_gb") is None:
                                    metadata["disk_usage_max_gb"] = round(stats["max"] / (1024**3), 2)
                                break
                
                # Collect IOPS metrics from v2 disk API
                try:
                    disks = self.client.get_disks(project_id, process_id)
                    if disks:
                        # Use the first disk partition
                        disk = disks[0]
                        partition_name = disk.get("partitionName")
                        if partition_name:
                            print(f"      Fetching IOPS from disk {partition_name}...")
                            iops_measurements = self.client.get_disk_measurements(
                                project_id, process_id, partition_name, granularity="PT1M", period="P2D"
                            )
                            if iops_measurements:
                                for measurement in iops_measurements.get("measurements", []):
                                    metric_name = measurement.get("name")
                                    if metric_name == "DISK_PARTITION_IOPS_TOTAL":
                                        stats = self.calculate_metric_stats_from_single(measurement)
                                        if stats["max"] is not None:
                                            metadata["iops_max"] = stats["max"]
                                            metadata["iops_avg"] = stats["avg"]
                                            break
                except Exception as e:
                    print(f"      Could not fetch IOPS metrics: {str(e)[:100]}")
                
                # Try DATABASE_OPERATIONS metrics for connections and operations
                op_measurements = self.client.get_process_measurements(
                    project_id, process_id, "DATABASE_OPERATIONS", granularity="PT1M", period="P2D"
                )
                if op_measurements:
                    # Connections
                    for measurement in op_measurements.get("measurements", []):
                        metric_name = measurement.get("name")
                        if metric_name == "CONNECTIONS":
                            stats = self.calculate_metric_stats_from_single(measurement)
                            if stats["max"] is not None:
                                metadata["connections_max"] = stats["max"]
                                metadata["connections_avg"] = stats["avg"]
                                break
                    
                    # Read operations - sum multiple metrics
                    read_op_metric_names = [
                        "OPCOUNTER_CMD", "OPCOUNTER_GETMORE", "OPCOUNTER_QUERY"
                    ]
                    read_op_metrics_to_sum = [
                        m for m in op_measurements.get("measurements", [])
                        if m.get("name") in read_op_metric_names
                    ]
                    if read_op_metrics_to_sum:
                        stats = self.calculate_metric_stats_from_multiple(read_op_metrics_to_sum)
                        if stats["max"] is not None:
                            metadata["read_ops_max"] = stats["max"]
                            metadata["read_ops_avg"] = stats["avg"]
                    
                    # Write operations - sum multiple metrics
                    write_op_metric_names = [
                        "OPCOUNTER_DELETE", "OPCOUNTER_TTL_DELETED", "OPCOUNTER_INSERT", "OPCOUNTER_UPDATE"
                    ]
                    write_op_metrics_to_sum = [
                        m for m in op_measurements.get("measurements", [])
                        if m.get("name") in write_op_metric_names
                    ]
                    if write_op_metrics_to_sum:
                        stats = self.calculate_metric_stats_from_multiple(write_op_metrics_to_sum)
                        if stats["max"] is not None:
                            metadata["write_ops_max"] = stats["max"]
                            metadata["write_ops_avg"] = stats["avg"]
            
        except Exception as e:
            print(f"      Metrics not available: {str(e)[:100]}")
        
        # Calculate disk available if we have both values
        disk_size = metadata.get("disk_size_gb")
        disk_usage = metadata.get("disk_usage_max_gb")
        if disk_size is not None and disk_usage is not None:
            metadata["disk_available_max_gb"] = round(disk_size - disk_usage, 2)
        
        # Calculate usage flags
        tier_specs = self.load_tier_specs()
        metadata = self.calculate_usage_flags(metadata, tier_specs)
        
        return metadata
    
    def collect_all_metadata(self) -> Dict:
        """Collect metadata for all projects and clusters"""
        print("Starting metadata collection...")
        print("=" * 80)
        
        projects = self.client.get_projects()
        print(f"Found {len(projects)} projects")
        print()
        
        results = {
            "organization_id": self.client.org_id,
            "collection_timestamp": datetime.now(timezone.utc).isoformat(),
            "projects": []
        }
        
        for project in projects:
            project_id = project["id"]
            project_name = project.get("name", "Unknown")
            
            print(f"Processing project: {project_name} ({project_id})")
            
            clusters = self.client.get_clusters(project_id)
            print(f"  Found {len(clusters)} clusters")
            
            cluster_metadata = []
            for cluster in clusters:
                try:
                    metadata = self.collect_cluster_metadata(project_id, cluster)
                    cluster_metadata.append(metadata)
                except Exception as e:
                    print(f"    Error collecting metadata for cluster {cluster.get('name')}: {e}")
            
            results["projects"].append({
                "project_id": project_id,
                "project_name": project_name,
                "clusters": cluster_metadata
            })
            
            print()
        
        print("=" * 80)
        print("Metadata collection complete!")
        
        return results


def main():
    parser = argparse.ArgumentParser(
        description="Collect metadata from MongoDB Atlas",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python atlas_metadata_collector.py --org-id 507f1f77bcf86cd799439011 \\
                                     --public-key my-public-key \\
                                     --private-key my-private-key \\
                                     --output results.json
                                     
  # Filter metrics to business hours only (2 PM to 11:59 PM)
  python atlas_metadata_collector.py --org-id 507f1f77bcf86cd799439011 \\
                                     --public-key my-public-key \\
                                     --private-key my-private-key \\
                                     --time-filter-start 14:00 \\
                                     --time-filter-end 23:59 \\
                                     --output results.csv

Environment variables:
  ATLAS_PUBLIC_KEY    MongoDB Atlas public API key
  ATLAS_PRIVATE_KEY   MongoDB Atlas private API key
  ATLAS_ORG_ID        MongoDB Atlas organization ID
        """
    )
    
    parser.add_argument("--org-id", type=str, default=os.getenv("ATLAS_ORG_ID"))
    parser.add_argument("--public-key", type=str, default=os.getenv("ATLAS_PUBLIC_KEY"))
    parser.add_argument("--private-key", type=str, default=os.getenv("ATLAS_PRIVATE_KEY"))
    parser.add_argument("--output", type=str, default="atlas_metadata.json")
    parser.add_argument("--pretty", action="store_true")
    parser.add_argument("--time-filter-start", type=str, help="Start time for filtering metrics (HH:MM format, e.g., 14:00)")
    parser.add_argument("--time-filter-end", type=str, help="End time for filtering metrics (HH:MM format, e.g., 23:59)")
    parser.add_argument("--tier-limits-file", type=str, default="tier_limits.csv", help="Path to tier limits configuration file")
    
    args = parser.parse_args()
    
    if not args.org_id:
        print("Error: --org-id is required")
        sys.exit(1)
    if not args.public_key:
        print("Error: --public-key is required")
        sys.exit(1)
    if not args.private_key:
        print("Error: --private-key is required")
        sys.exit(1)
    
    # Validate time filter arguments
    time_filter = None
    if args.time_filter_start or args.time_filter_end:
        if not (args.time_filter_start and args.time_filter_end):
            print("Error: Both --time-filter-start and --time-filter-end must be provided together")
            sys.exit(1)
        
        # Validate time format
        try:
            time.fromisoformat(args.time_filter_start)
            time.fromisoformat(args.time_filter_end)
            time_filter = (args.time_filter_start, args.time_filter_end)
            print(f"Time filter enabled: {args.time_filter_start} - {args.time_filter_end}")
        except ValueError:
            print("Error: Time filter values must be in HH:MM format (e.g., 14:00)")
            sys.exit(1)
    
    try:
        collector = AtlasMetadataCollector(
            args.public_key, 
            args.private_key, 
            args.org_id, 
            time_filter=time_filter,
            tier_limits_file=args.tier_limits_file
        )
        results = collector.collect_all_metadata()
        
        # Detect output format based on file extension
        output_file = args.output
        is_json = output_file.lower().endswith('.json')
        is_csv = output_file.lower().endswith('.csv')
        
        if is_json:
            # Write JSON output
            with open(output_file, 'w') as f:
                if args.pretty:
                    json.dump(results, f, indent=2)
                else:
                    json.dump(results, f)
        elif is_csv:
            # Write CSV output
            with open(output_file, 'w', newline='') as f:
                writer = csv.writer(f)
                
                # Write header - ordered by first part before underscore, with "_use" columns at end
                writer.writerow([
                    # Basic cluster info (no underscore grouping)
                    'project_name', 'project_id', 'cluster_name', 'cluster_id',
                    'cluster_type', 'mongodb_version', 'state', 'provider', 'region',
                    'tier', 'disk_size_gb', 'created_at', 'updated_at',
                    
                    # CPU metrics grouped together
                    'cpu_max_percent', 'cpu_avg_percent', 'cpu_tier_limit', 'cpu_lower_tier_limit',
                    
                    # Memory metrics grouped together  
                    'memory_max_gb', 'memory_avg_gb', 'memory_tier_limit_gb', 'memory_lower_tier_limit_gb',
                    
                    # IOPS metrics grouped together
                    'iops_max', 'iops_avg', 'iops_tier_limit', 'iops_lower_tier_limit',
                    
                    # Connection metrics grouped together
                    'connections_max', 'connections_avg', 'connections_tier_limit', 'connections_lower_tier_limit',
                    
                    # Disk metrics grouped together
                    'disk_usage_max_gb', 'disk_available_max_gb', 'disk_tier_limit_gb', 'disk_lower_tier_limit_gb',
                    
                    # Read operations grouped together
                    'read_ops_max', 'read_ops_avg',
                    
                    # Write operations grouped together
                    'write_ops_max', 'write_ops_avg',
                    
                    # All "_use" columns at the end
                    'low_cpu_use', 'low_memory_use', 'low_iops_use', 'low_connections_use', 'low_disk_use',
                    'cpu_lower_tier_acceptable_use', 'memory_lower_tier_acceptable_use', 'iops_lower_tier_acceptable_use', 'connections_lower_tier_acceptable_use', 'disk_lower_tier_acceptable_use'
                ])
                
                # Write cluster data - same order as header
                for project in results["projects"]:
                    project_name = project["project_name"]
                    project_id = project["project_id"]
                    
                    for cluster in project["clusters"]:
                        writer.writerow([
                            # Basic cluster info
                            project_name,
                            project_id,
                            cluster.get("cluster_name"),
                            cluster.get("cluster_id"),
                            cluster.get("cluster_type"),
                            cluster.get("mongodb_version"),
                            cluster.get("state"),
                            cluster.get("provider"),
                            cluster.get("region"),
                            cluster.get("tier"),
                            cluster.get("disk_size_gb"),
                            cluster.get("created_at"),
                            cluster.get("updated_at"),
                            
                            # CPU metrics grouped together
                            cluster.get("cpu_max_percent"),
                            cluster.get("cpu_avg_percent"),
                            cluster.get("cpu_tier_limit"),
                            cluster.get("cpu_lower_tier_limit"),
                            
                            # Memory metrics grouped together
                            cluster.get("memory_max_gb"),
                            cluster.get("memory_avg_gb"),
                            cluster.get("memory_tier_limit_gb"),
                            cluster.get("memory_lower_tier_limit_gb"),
                            
                            # IOPS metrics grouped together
                            cluster.get("iops_max"),
                            cluster.get("iops_avg"),
                            cluster.get("iops_tier_limit"),
                            cluster.get("iops_lower_tier_limit"),
                            
                            # Connection metrics grouped together
                            cluster.get("connections_max"),
                            cluster.get("connections_avg"),
                            cluster.get("connections_tier_limit"),
                            cluster.get("connections_lower_tier_limit"),
                            
                            # Disk metrics grouped together
                            cluster.get("disk_usage_max_gb"),
                            cluster.get("disk_available_max_gb"),
                            cluster.get("disk_tier_limit_gb"),
                            cluster.get("disk_lower_tier_limit_gb"),
                            
                            # Read operations grouped together
                            cluster.get("read_ops_max"),
                            cluster.get("read_ops_avg"),
                            
                            # Write operations grouped together
                            cluster.get("write_ops_max"),
                            cluster.get("write_ops_avg"),
                            
                            # All "_use" columns at the end
                            cluster.get("low_cpu_use"),
                            cluster.get("low_memory_use"),
                            cluster.get("low_iops_use"),
                            cluster.get("low_connections_use"),
                            cluster.get("low_disk_use"),
                            cluster.get("cpu_lower_tier_acceptable_use"),
                            cluster.get("memory_lower_tier_acceptable_use"),
                            cluster.get("iops_lower_tier_acceptable_use"),
                            cluster.get("connections_lower_tier_acceptable_use"),
                            cluster.get("disk_lower_tier_acceptable_use")
                        ])
        else:
            # Default to JSON if extension is not recognized
            with open(output_file, 'w') as f:
                json.dump(results, f)
        
        print(f"\nResults written to: {output_file}")
        
        total_clusters = sum(len(p["clusters"]) for p in results["projects"])
        print(f"Total clusters processed: {total_clusters}")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()



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
from datetime import datetime, timezone
from typing import Dict, List, Optional
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
        endpoint = f"/groups/{project_id}/processes/{process_id}/disks/{partition_name}"
        # Use v2 API endpoint
        url = f"https://cloud.mongodb.com/api/atlas/v2{endpoint}"
        try:
            # v2 API requires special Accept header
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
    
    def __init__(self, public_key: str, private_key: str, org_id: str):
        self.client = AtlasAPIClient(public_key, private_key, org_id)
    
    def calculate_metric_stats_from_single(self, measurement: Dict) -> Dict[str, float]:
        """Calculate max and avg for a single measurement object"""
        data_points = []
        
        for datapoint in measurement.get("dataPoints", []):
            if datapoint.get("value") is not None:
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
        # Collect all timestamps
        all_timestamps = set()
        for measurement in measurements:
            for datapoint in measurement.get("dataPoints", []):
                if datapoint.get("timestamp"):
                    all_timestamps.add(datapoint["timestamp"])
        
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
            "cpu_max_week": None,
            "cpu_avg_week": None,
            "memory_max_week": None,
            "memory_avg_week": None,
            "iops_max_week": None,
            "iops_avg_week": None,
            "connections_max_week": None,
            "connections_avg_week": None,
            "operations_max_week": None,
            "operations_avg_week": None,
            "disk_usage_max_gb": None,
            "disk_available_max_gb": None,
        })
        
        # Try to fetch metrics if available
        try:
            print(f"      Attempting to fetch metrics...")
            processes = self.client.get_processes(project_id)
            
            if processes:
                # Try to find the primary process, otherwise use the first one
                primary_process = None
                for p in processes:
                    if p.get("typeName") == "REPLICA_PRIMARY":
                        primary_process = p
                        break
                
                if not primary_process:
                    primary_process = processes[0]
                
                process_id = primary_process["id"]
                process_type = primary_process.get("typeName", "UNKNOWN")
                print(f"      Using process: {process_id} ({process_type})")
                
                # Collect CPU metrics - sum multiple metrics
                cpu_measurements = self.client.get_process_measurements(
                    project_id, process_id, "CPU_USAGE", granularity="PT1H", period="P7D"
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
                            metadata["cpu_max_week"] = stats["max"]
                            metadata["cpu_avg_week"] = stats["avg"]
                
                # Collect MEMORY metrics
                memory_measurements = self.client.get_process_measurements(
                    project_id, process_id, "MEMORY", granularity="PT1H", period="P7D"
                )
                if memory_measurements:
                    for measurement in memory_measurements.get("measurements", []):
                        metric_name = measurement.get("name")
                        if metric_name == "SYSTEM_MEMORY_USED":
                            stats = self.calculate_metric_stats_from_single(measurement)
                            if stats["max"] is not None:
                                # Convert bytes to GB
                                metadata["memory_max_week"] = round(stats["max"] / (1024**3), 2)
                                metadata["memory_avg_week"] = round(stats["avg"] / (1024**3), 2)
                                break
                
                # Collect DISK and DATABASE_SIZE metrics
                disk_measurements = self.client.get_process_measurements(
                    project_id, process_id, "DISK", granularity="PT1H", period="P7D"
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
                    project_id, process_id, "DATABASE_SIZE", granularity="PT1H", period="P7D"
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
                                project_id, process_id, partition_name, granularity="PT1H", period="P7D"
                            )
                            if iops_measurements:
                                for measurement in iops_measurements.get("measurements", []):
                                    metric_name = measurement.get("name")
                                    if metric_name == "DISK_PARTITION_IOPS_TOTAL":
                                        stats = self.calculate_metric_stats_from_single(measurement)
                                        if stats["max"] is not None:
                                            metadata["iops_max_week"] = stats["max"]
                                            metadata["iops_avg_week"] = stats["avg"]
                                            break
                except Exception as e:
                    print(f"      Could not fetch IOPS metrics: {str(e)[:100]}")
                
                # Try DATABASE_OPERATIONS metrics for connections and operations
                op_measurements = self.client.get_process_measurements(
                    project_id, process_id, "DATABASE_OPERATIONS", granularity="PT1H", period="P7D"
                )
                if op_measurements:
                    # Connections
                    for measurement in op_measurements.get("measurements", []):
                        metric_name = measurement.get("name")
                        if metric_name == "CONNECTIONS":
                            stats = self.calculate_metric_stats_from_single(measurement)
                            if stats["max"] is not None:
                                metadata["connections_max_week"] = stats["max"]
                                metadata["connections_avg_week"] = stats["avg"]
                                break
                    
                    # Operations - sum multiple metrics
                    op_metric_names = [
                        "OPCOUNTER_CMD", "OPCOUNTER_DELETE", "OPCOUNTER_TTL_DELETED",
                        "OPCOUNTER_GETMORE", "OPCOUNTER_INSERT", "OPCOUNTER_QUERY", "OPCOUNTER_UPDATE"
                    ]
                    op_metrics_to_sum = [
                        m for m in op_measurements.get("measurements", [])
                        if m.get("name") in op_metric_names
                    ]
                    if op_metrics_to_sum:
                        stats = self.calculate_metric_stats_from_multiple(op_metrics_to_sum)
                        if stats["max"] is not None:
                            metadata["operations_max_week"] = stats["max"]
                            metadata["operations_avg_week"] = stats["avg"]
            
        except Exception as e:
            print(f"      Metrics not available: {str(e)[:100]}")
        
        # Calculate disk available if we have both values
        if metadata.get("disk_size_gb") and metadata.get("disk_usage_max_gb"):
            metadata["disk_available_max_gb"] = round(
                metadata["disk_size_gb"] - metadata["disk_usage_max_gb"], 2
            )
        
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
    
    try:
        collector = AtlasMetadataCollector(args.public_key, args.private_key, args.org_id)
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
                
                # Write header
                writer.writerow([
                    'project_name', 'project_id', 'cluster_name', 'cluster_id',
                    'cluster_type', 'mongodb_version', 'state', 'provider', 'region',
                    'tier', 'disk_size_gb', 'created_at', 'updated_at',
                    'cpu_max_week', 'cpu_avg_week', 'memory_max_week', 'memory_avg_week',
                    'iops_max_week', 'iops_avg_week', 'connections_max_week', 'connections_avg_week',
                    'operations_max_week', 'operations_avg_week', 'disk_usage_max_gb', 'disk_available_max_gb'
                ])
                
                # Write cluster data
                for project in results["projects"]:
                    project_name = project["project_name"]
                    project_id = project["project_id"]
                    
                    for cluster in project["clusters"]:
                        writer.writerow([
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
                            cluster.get("cpu_max_week"),
                            cluster.get("cpu_avg_week"),
                            cluster.get("memory_max_week"),
                            cluster.get("memory_avg_week"),
                            cluster.get("iops_max_week"),
                            cluster.get("iops_avg_week"),
                            cluster.get("connections_max_week"),
                            cluster.get("connections_avg_week"),
                            cluster.get("operations_max_week"),
                            cluster.get("operations_avg_week"),
                            cluster.get("disk_usage_max_gb"),
                            cluster.get("disk_available_max_gb")
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



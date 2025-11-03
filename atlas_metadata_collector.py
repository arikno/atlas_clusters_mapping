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
                # Use the first process to try fetching metrics
                process = processes[0]
                process_id = process["id"]
                print(f"      Using process: {process_id}")
                
                # Try CPU_USAGE metrics
                cpu_measurements = self.client.get_process_measurements(
                    project_id, process_id, "CPU_USAGE", granularity="PT1H", period="P7D"
                )
                if cpu_measurements:
                    for measurement in cpu_measurements.get("measurements", []):
                        metric_name = measurement.get("name")
                        if metric_name in ["PROCESS_NORMALIZED_CPU_USER", "PROCESS_CPU_USER"]:
                            stats = self.calculate_metric_stats_from_single(measurement)
                            if stats["max"] is not None:
                                metadata["cpu_max_week"] = stats["max"]
                                metadata["cpu_avg_week"] = stats["avg"]
                                break
                
                # Try MEMORY metrics
                memory_measurements = self.client.get_process_measurements(
                    project_id, process_id, "MEMORY", granularity="PT1H", period="P7D"
                )
                if memory_measurements:
                    for measurement in memory_measurements.get("measurements", []):
                        metric_name = measurement.get("name")
                        if metric_name in ["PROCESS_VIRTUAL_MEMORY", "PROCESS_RESIDENT_MEMORY"]:
                            stats = self.calculate_metric_stats_from_single(measurement)
                            if stats["max"] is not None:
                                # Convert bytes to GB
                                metadata["memory_max_week"] = round(stats["max"] / (1024**3), 2)
                                metadata["memory_avg_week"] = round(stats["avg"] / (1024**3), 2)
                                break
                
                # Try DISK metrics
                disk_measurements = self.client.get_process_measurements(
                    project_id, process_id, "DISK", granularity="PT1H", period="P7D"
                )
                if disk_measurements:
                    for measurement in disk_measurements.get("measurements", []):
                        metric_name = measurement.get("name")
                        if "DISK" in metric_name and "USAGE" in metric_name:
                            stats = self.calculate_metric_stats_from_single(measurement)
                            if stats["max"] is not None:
                                # Convert bytes to GB
                                metadata["disk_usage_max_gb"] = round(stats["max"] / (1024**3), 2)
                                break
                
                # Try DATABASE_OPERATIONS metrics
                op_measurements = self.client.get_process_measurements(
                    project_id, process_id, "DATABASE_OPERATIONS", granularity="PT1H", period="P7D"
                )
                if op_measurements:
                    for measurement in op_measurements.get("measurements", []):
                        metric_name = measurement.get("name")
                        if metric_name == "CONNECTIONS":
                            stats = self.calculate_metric_stats_from_single(measurement)
                            if stats["max"] is not None:
                                metadata["connections_max_week"] = stats["max"]
                                metadata["connections_avg_week"] = stats["avg"]
                                break
            
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
        
        with open(args.output, 'w') as f:
            if args.pretty:
                json.dump(results, f, indent=2)
            else:
                json.dump(results, f)
        
        print(f"\nResults written to: {args.output}")
        
        total_clusters = sum(len(p["clusters"]) for p in results["projects"])
        print(f"Total clusters processed: {total_clusters}")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()


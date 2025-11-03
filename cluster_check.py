#!/usr/bin/env python3
"""
MongoDB Atlas Cluster Checker with Full Metrics

This script checks and lists all clusters in a specific MongoDB Atlas project
with full metrics collection (CPU, Memory, IOPS, Disk, Connections, Operations).

Requirements:
    pip install requests python-dotenv

Usage:
    python cluster_check.py
"""

import csv
import json
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional
import requests


class AtlasClusterChecker:
    """Check clusters in a MongoDB Atlas project with full metrics"""
    
    BASE_URL = "https://cloud.mongodb.com/api/atlas/v1.0"
    
    def __init__(self, public_key: str, private_key: str, project_id: str):
        self.public_key = public_key
        self.private_key = private_key
        self.project_id = project_id
        self.session = requests.Session()
        self.session.auth = requests.auth.HTTPDigestAuth(public_key, private_key)
    
    def _get(self, endpoint: str, params: Optional[Dict] = None, raise_on_error: bool = True) -> Optional[Dict]:
        """Make a GET request to Atlas API"""
        url = f"{self.BASE_URL}{endpoint}"
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if raise_on_error:
                print(f"HTTP Error: {e}")
                if hasattr(response, 'text'):
                    print(f"Response: {response.text}")
                raise
            return None
        except requests.exceptions.RequestException as e:
            if raise_on_error:
                print(f"Request Error: {e}")
                raise
            return None
    
    def get_clusters(self) -> List[Dict]:
        """Get all clusters for the project"""
        response = self._get(f"/groups/{self.project_id}/clusters")
        return response.get("results", [])
    
    def get_processes(self) -> List[Dict]:
        """Get all processes for the project"""
        response = self._get(f"/groups/{self.project_id}/processes", raise_on_error=False)
        if not response:
            return []
        return response.get("results", [])
    
    def get_process_measurements(self, process_id: str, measurement_type: str,
                                granularity: str = "PT1H", period: str = "P7D") -> Optional[Dict]:
        """Get process-level measurements"""
        params = {
            'granularity': granularity,
            'period': period,
            'measurementType': measurement_type,
        }
        endpoint = f"/groups/{self.project_id}/processes/{process_id}/measurements"
        return self._get(endpoint, params=params, raise_on_error=False)
    
    def get_disks(self, process_id: str) -> List[Dict]:
        """Get all disks for a process using v2 API"""
        endpoint = f"/groups/{self.project_id}/processes/{process_id}/disks"
        url = f"https://cloud.mongodb.com/api/atlas/v2{endpoint}"
        try:
            headers = {"Accept": "application/vnd.atlas.2025-11-02+json"}
            response = self.session.get(url, headers=headers)
            response.raise_for_status()
            result = response.json()
            return result.get("results", [])
        except requests.exceptions.RequestException:
            return []
    
    def get_disk_measurements(self, process_id: str, partition_name: str,
                             granularity: str = "PT1H", period: str = "P7D") -> Optional[Dict]:
        """Get disk-level measurements using v2 API"""
        params = {
            'granularity': granularity,
            'period': period,
            'measurementTypes': 'DISK_PARTITION_IOPS_TOTAL'
        }
        # Correct endpoint: /disks/{partition}/measurements
        endpoint = f"/groups/{self.project_id}/processes/{process_id}/disks/{partition_name}/measurements"
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
        """Calculate max and avg by summing multiple metrics at each timestamp"""
        all_timestamps = set()
        for measurement in measurements:
            for datapoint in measurement.get("dataPoints", []):
                if datapoint.get("timestamp"):
                    all_timestamps.add(datapoint["timestamp"])
        
        if not all_timestamps:
            return {"max": None, "avg": None, "data_point_count": 0}
        
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
    
    def collect_metrics(self, cluster: Dict) -> Dict:
        """Collect metrics for a cluster"""
        metrics = {
            "cpu_max_percent": None,
            "cpu_avg_percent": None,
            "memory_max_gb": None,
            "memory_avg_gb": None,
            "iops_max_week": None,
            "iops_avg_week": None,
            "connections_max_week": None,
            "connections_avg_week": None,
            "read_ops_max_week": None,
            "read_ops_avg_week": None,
            "write_ops_max_week": None,
            "write_ops_avg_week": None,
            "disk_usage_max_gb": None,
            "disk_available_max_gb": None,
            "low_memory_use": None,
            "low_iops_use": None,
            "low_cpu_use": None,
        }
        
        try:
            processes = self.get_processes()
            
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
                
                process_id = primary_process["id"]
                
                # Collect CPU metrics - sum multiple metrics
                cpu_measurements = self.get_process_measurements(
                    process_id, "CPU_USAGE", granularity="PT1M", period="P2D"
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
                            metrics["cpu_max_percent"] = stats["max"]
                            metrics["cpu_avg_percent"] = stats["avg"]
                
                # Collect MEMORY metrics
                memory_measurements = self.get_process_measurements(
                    process_id, "MEMORY", granularity="PT1M", period="P2D"
                )
                if memory_measurements:
                    for measurement in memory_measurements.get("measurements", []):
                        metric_name = measurement.get("name")
                        if metric_name == "SYSTEM_MEMORY_USED":
                            stats = self.calculate_metric_stats_from_single(measurement)
                            if stats["max"] is not None:
                                # SYSTEM_MEMORY_USED is in KB, convert to GB
                                metrics["memory_max_gb"] = round(stats["max"] / (1024**2), 2)
                                metrics["memory_avg_gb"] = round(stats["avg"] / (1024**2), 2)
                                break
                
                # Collect DISK metrics
                disk_measurements = self.get_process_measurements(
                    process_id, "DISK", granularity="PT1M", period="P2D"
                )
                if disk_measurements:
                    for measurement in disk_measurements.get("measurements", []):
                        metric_name = measurement.get("name")
                        if metric_name == "DB_STORAGE_TOTAL":
                            stats = self.calculate_metric_stats_from_single(measurement)
                            if stats["max"] is not None:
                                metrics["disk_usage_max_gb"] = round(stats["max"] / (1024**3), 2)
                                break
                
                # Try DATABASE_SIZE for DB_DATA_SIZE_TOTAL
                db_size_measurements = self.get_process_measurements(
                    process_id, "DATABASE_SIZE", granularity="PT1M", period="P2D"
                )
                if db_size_measurements:
                    for measurement in db_size_measurements.get("measurements", []):
                        metric_name = measurement.get("name")
                        if metric_name == "DB_DATA_SIZE_TOTAL":
                            stats = self.calculate_metric_stats_from_single(measurement)
                            if stats["max"] is not None:
                                if metrics["disk_usage_max_gb"] is None:
                                    metrics["disk_usage_max_gb"] = round(stats["max"] / (1024**3), 2)
                                break
                
                # Collect IOPS metrics from v2 disk API
                try:
                    disks = self.get_disks(process_id)
                    if disks:
                        disk = disks[0]
                        partition_name = disk.get("partitionName")
                        if partition_name:
                            iops_measurements = self.get_disk_measurements(
                                process_id, partition_name, granularity="PT1M", period="P2D"
                            )
                            if iops_measurements:
                                for measurement in iops_measurements.get("measurements", []):
                                    metric_name = measurement.get("name")
                                    if metric_name == "DISK_PARTITION_IOPS_TOTAL":
                                        stats = self.calculate_metric_stats_from_single(measurement)
                                        if stats["max"] is not None:
                                            metrics["iops_max_week"] = stats["max"]
                                            metrics["iops_avg_week"] = stats["avg"]
                                            break
                except Exception as e:
                    pass
                
                # Try DATABASE_OPERATIONS metrics for connections and operations
                op_measurements = self.get_process_measurements(
                    process_id, "DATABASE_OPERATIONS", granularity="PT1M", period="P2D"
                )
                if op_measurements:
                    # Connections
                    for measurement in op_measurements.get("measurements", []):
                        metric_name = measurement.get("name")
                        if metric_name == "CONNECTIONS":
                            stats = self.calculate_metric_stats_from_single(measurement)
                            if stats["max"] is not None:
                                metrics["connections_max_week"] = stats["max"]
                                metrics["connections_avg_week"] = stats["avg"]
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
                            metrics["read_ops_max_week"] = stats["max"]
                            metrics["read_ops_avg_week"] = stats["avg"]
                    
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
                            metrics["write_ops_max_week"] = stats["max"]
                            metrics["write_ops_avg_week"] = stats["avg"]
            
        except Exception:
            pass
        
        return metrics
    
    def load_tier_specs(self) -> Dict:
        """Load tier specifications from CSV file"""
        tier_specs = {}
        try:
            with open('atlas tiers aws - sheet1.csv', 'r') as f:
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
                            'connection': float(row.get('connection', 0)),
                            'iops': float(row.get('iops', 0))
                        }
        except FileNotFoundError:
            print("Warning: tier specs file not found")
        return tier_specs
    
    def calculate_usage_flags(self, cluster_info: Dict, tier_specs: Dict) -> Dict:
        """Calculate low usage flags based on tier specifications"""
        tier = cluster_info.get("tier")
        if not tier or tier not in tier_specs:
            return cluster_info
        
        spec = tier_specs[tier]
        
        # Calculate memory usage percentage
        memory_avg = cluster_info.get("memory_avg_gb")
        ram_limit = spec.get("ram")
        if memory_avg is not None and ram_limit:
            memory_usage_percent = (memory_avg / ram_limit) * 100
            cluster_info["low_memory_use"] = True if memory_usage_percent < 40 else None
        
        # Calculate IOPS usage percentage
        iops_avg = cluster_info.get("iops_avg_week")
        iops_limit = spec.get("iops")
        if iops_avg is not None and iops_limit:
            iops_usage_percent = (iops_avg / iops_limit) * 100
            cluster_info["low_iops_use"] = True if iops_usage_percent < 40 else None
        
        # Calculate CPU usage percentage
        cpu_avg = cluster_info.get("cpu_avg_percent")
        if cpu_avg is not None:
            cluster_info["low_cpu_use"] = True if cpu_avg < 40 else None
        
        return cluster_info
    
    def check_clusters(self) -> Dict:
        """Check all clusters in the project"""
        print("=" * 80)
        print(f"Checking clusters in project: {self.project_id}")
        print("=" * 80)
        
        clusters = self.get_clusters()
        print(f"\nFound {len(clusters)} clusters\n")
        
        # Load tier specs once for all clusters
        tier_specs = self.load_tier_specs()
        
        cluster_list = []
        for idx, cluster in enumerate(clusters, 1):
            cluster_name = cluster.get("name")
            print(f"[{idx}/{len(clusters)}] Processing cluster: {cluster_name}")
            
            cluster_info = {
                "cluster_name": cluster_name,
                "cluster_id": cluster.get("id"),
                "cluster_type": cluster.get("clusterType"),
                "mongodb_version": cluster.get("mongoDBVersion"),
                "state": cluster.get("stateName"),
                "provider": cluster.get("providerName"),
                "region": cluster.get("providerRegionName"),
                "tier": None,
                "disk_size_gb": cluster.get("diskSizeGB"),
                "created_at": cluster.get("createDate"),
            }
            
            # Extract tier from replicationSpecs
            replication_specs = cluster.get("replicationSpecs", [])
            if replication_specs and len(replication_specs) > 0:
                regions_config = replication_specs[0].get("regionsConfig", {})
                if regions_config:
                    first_config = list(regions_config.values())[0]
                    if "electableSpecs" in first_config and len(first_config["electableSpecs"]) > 0:
                        cluster_info["tier"] = first_config["electableSpecs"][0].get("instanceSize", None)
                    elif "readOnlySpecs" in first_config and len(first_config["readOnlySpecs"]) > 0:
                        cluster_info["tier"] = first_config["readOnlySpecs"][0].get("instanceSize", None)
                    elif "analyticsSpecs" in first_config and len(first_config["analyticsSpecs"]) > 0:
                        cluster_info["tier"] = first_config["analyticsSpecs"][0].get("instanceSize", None)
            
            # Also check providerSettings for tier
            provider_settings = cluster.get("providerSettings", {})
            if provider_settings:
                cluster_info["provider"] = provider_settings.get("providerName")
                cluster_info["region"] = provider_settings.get("regionName")
                if not cluster_info.get("tier"):
                    cluster_info["tier"] = provider_settings.get("instanceSizeName")
            
            # Collect metrics
            print(f"  Collecting metrics...")
            metrics = self.collect_metrics(cluster)
            cluster_info.update(metrics)
            
            # Calculate disk available
            if cluster_info.get("disk_size_gb") and cluster_info.get("disk_usage_max_gb"):
                cluster_info["disk_available_max_gb"] = round(
                    cluster_info["disk_size_gb"] - cluster_info["disk_usage_max_gb"], 2
                )
            
            # Calculate usage flags
            cluster_info = self.calculate_usage_flags(cluster_info, tier_specs)
            
            cluster_list.append(cluster_info)
            
            # Print summary
            print(f"  ✓ State: {cluster_info['state']}, Tier: {cluster_info['tier']}")
            if cluster_info.get("cpu_max_percent") is not None:
                print(f"    CPU: {cluster_info['cpu_max_percent']}/{cluster_info['cpu_avg_percent']}% (max/avg)")
            if cluster_info.get("memory_max_gb") is not None:
                print(f"    Memory: {cluster_info['memory_max_gb']}/{cluster_info['memory_avg_gb']} GB (max/avg)")
            print()
        
        return {
            "project_id": self.project_id,
            "check_timestamp": datetime.now(timezone.utc).isoformat(),
            "total_clusters": len(cluster_list),
            "clusters": cluster_list
        }


def main():
    """Main entry point"""
    # Hardcoded credentials for this specific project
    PUBLIC_KEY = "rsklcwxl"
    PRIVATE_KEY = "5c618ca7-c7b6-4f76-9952-3f196e15d83c"
    PROJECT_ID = "6807f16c3b67b24d31fb8bf2"
    
    try:
        checker = AtlasClusterChecker(PUBLIC_KEY, PRIVATE_KEY, PROJECT_ID)
        results = checker.check_clusters()
        
        # Save results to file
        with open("clusters_check.json", "w") as f:
            json.dump(results, f, indent=2)
        
        print("=" * 80)
        print(f"✓ Cluster check complete!")
        print(f"  Total clusters: {results['total_clusters']}")
        print(f"  Results saved to: clusters_check.json")
        print("=" * 80)
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

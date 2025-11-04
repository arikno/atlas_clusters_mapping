#!/usr/bin/env python3
"""
Comprehensive test script for all new Atlas Metadata Collector functionality
"""

from datetime import datetime, time
import sys
import os
sys.path.append('.')

from atlas_metadata_collector import AtlasMetadataCollector

def test_time_filtering():
    """Test time-based metrics filtering functionality"""
    print("=" * 60)
    print("TESTING: Time Filtering Functionality")
    print("=" * 60)
    
    # Test 1: Business hours filter (14:00-23:59)
    collector = AtlasMetadataCollector(
        "test_key", "test_secret", "test_org", 
        time_filter=("14:00", "23:59")
    )
    
    test_cases = [
        ("2023-10-01T10:30:00Z", False, "Before business hours"),
        ("2023-10-01T14:00:00Z", True, "Start of business hours"),
        ("2023-10-01T18:30:00Z", True, "During business hours"),
        ("2023-10-01T23:59:00Z", True, "End of business hours"),
        ("2023-10-01T02:00:00Z", False, "After business hours"),
        ("2023-10-01T13:59:00Z", False, "Just before start"),
        ("2023-10-01T00:00:00Z", False, "Midnight"),
    ]
    
    print("Business hours filter (14:00-23:59):")
    all_passed = True
    for timestamp, expected, description in test_cases:
        result = collector._is_within_time_filter(timestamp)
        status = "✓" if result == expected else "✗"
        if result != expected:
            all_passed = False
        print(f"  {status} {timestamp} -> {result} (expected {expected}) - {description}")
    
    # Test 2: Cross-midnight filter (22:00-06:00)
    print("\nCross-midnight filter (22:00-06:00):")
    collector_midnight = AtlasMetadataCollector(
        "test_key", "test_secret", "test_org", 
        time_filter=("22:00", "06:00")
    )
    
    midnight_cases = [
        ("2023-10-01T21:59:00Z", False, "Just before night shift"),
        ("2023-10-01T22:00:00Z", True, "Start of night shift"),
        ("2023-10-01T02:30:00Z", True, "Middle of night"),
        ("2023-10-01T06:00:00Z", True, "End of night shift"),
        ("2023-10-01T06:01:00Z", False, "Just after night shift"),
        ("2023-10-01T12:00:00Z", False, "Midday"),
    ]
    
    for timestamp, expected, description in midnight_cases:
        result = collector_midnight._is_within_time_filter(timestamp)
        status = "✓" if result == expected else "✗"
        if result != expected:
            all_passed = False
        print(f"  {status} {timestamp} -> {result} (expected {expected}) - {description}")
    
    # Test 3: No filter (should always return True)
    print("\nNo time filter (should always be True):")
    collector_no_filter = AtlasMetadataCollector("test_key", "test_secret", "test_org")
    
    for timestamp, _, description in test_cases[:3]:
        result = collector_no_filter._is_within_time_filter(timestamp)
        status = "✓" if result == True else "✗"
        if result != True:
            all_passed = False
        print(f"  {status} {timestamp} -> {result} (expected True) - {description}")
    
    print(f"\nTime filtering tests: {'PASSED' if all_passed else 'FAILED'}")
    return all_passed

def test_tier_limits_loading():
    """Test configurable tier limits loading"""
    print("\n" + "=" * 60)
    print("TESTING: Tier Limits Configuration Loading")
    print("=" * 60)
    
    collector = AtlasMetadataCollector(
        "test_key", "test_secret", "test_org",
        tier_limits_file="tier_limits.csv"
    )
    
    limits = collector.load_tier_limits()
    
    print("Loaded tier limits:")
    expected_structure = {
        'cpu': {'low_usage': float, 'lower_tier': float},
        'memory': {'low_usage': float, 'lower_tier': float},
        'iops': {'low_usage': float, 'lower_tier': float},
        'connections': {'low_usage': float, 'lower_tier': float},
        'disk': {'low_usage': float, 'lower_tier': float}
    }
    
    all_passed = True
    for metric, thresholds in limits.items():
        if metric in expected_structure:
            low_usage = thresholds.get('low_usage')
            lower_tier = thresholds.get('lower_tier')
            print(f"  {metric}: low_usage={low_usage}%, lower_tier={lower_tier}%")
            
            # Verify structure
            if not isinstance(thresholds, dict) or 'low_usage' not in thresholds or 'lower_tier' not in thresholds:
                print(f"    ✗ Invalid structure for {metric}")
                all_passed = False
            elif not isinstance(low_usage, (int, float)) or not isinstance(lower_tier, (int, float)):
                print(f"    ✗ Invalid types for {metric}")
                all_passed = False
            else:
                print(f"    ✓ Structure valid")
        else:
            print(f"  ✗ Unexpected metric: {metric}")
            all_passed = False
    
    print(f"\nTier limits loading: {'PASSED' if all_passed else 'FAILED'}")
    return all_passed

def test_tier_specs_loading():
    """Test tier specifications loading with connections field"""
    print("\n" + "=" * 60)
    print("TESTING: Tier Specifications Loading")
    print("=" * 60)
    
    collector = AtlasMetadataCollector("test_key", "test_secret", "test_org")
    tier_specs = collector.load_tier_specs()
    
    print("Loaded tier specifications:")
    expected_tiers = ['M10', 'M20', 'M30', 'M40', 'M50', 'M60', 'M80', 'M140', 'M200']
    expected_fields = ['cpu', 'ram', 'connections', 'iops', 'sort']
    
    all_passed = True
    for tier_name in expected_tiers:
        if tier_name in tier_specs:
            spec = tier_specs[tier_name]
            print(f"  {tier_name}: cpu={spec.get('cpu')}, ram={spec.get('ram')}GB, "
                  f"connections={spec.get('connections')}, iops={spec.get('iops')}, sort={spec.get('sort')}")
            
            # Verify all fields exist
            for field in expected_fields:
                if field not in spec:
                    print(f"    ✗ Missing field: {field}")
                    all_passed = False
                elif not isinstance(spec[field], (int, float)):
                    print(f"    ✗ Invalid type for {field}: {type(spec[field])}")
                    all_passed = False
            
            # Verify sort order makes sense (should be 1-9)
            if spec.get('sort') not in range(1, 10):
                print(f"    ✗ Invalid sort value: {spec.get('sort')}")
                all_passed = False
        else:
            print(f"  ✗ Missing tier: {tier_name}")
            all_passed = False
    
    print(f"\nTier specs loading: {'PASSED' if all_passed else 'FAILED'}")
    return all_passed

def test_lower_tier_finding():
    """Test lower tier identification logic"""
    print("\n" + "=" * 60)
    print("TESTING: Lower Tier Finding Logic")
    print("=" * 60)
    
    collector = AtlasMetadataCollector("test_key", "test_secret", "test_org")
    tier_specs = collector.load_tier_specs()
    
    test_cases = [
        ("M10", None, "Lowest tier has no lower tier"),
        ("M20", "M10", "M20 should downgrade to M10"),
        ("M30", "M20", "M30 should downgrade to M20"),
        ("M50", "M40", "M50 should downgrade to M40"),
        ("M200", "M140", "Highest tier should downgrade to M140"),
        ("INVALID", None, "Invalid tier should return None"),
    ]
    
    all_passed = True
    for current_tier, expected_lower, description in test_cases:
        lower_spec = collector.find_lower_tier(current_tier, tier_specs)
        
        if expected_lower is None:
            result = lower_spec is None
            actual = None
        else:
            # Find the tier name that matches the lower_spec
            actual = None
            if lower_spec:
                for tier_name, spec in tier_specs.items():
                    if spec == lower_spec:
                        actual = tier_name
                        break
            result = actual == expected_lower
        
        status = "✓" if result else "✗"
        if not result:
            all_passed = False
        print(f"  {status} {current_tier} -> {actual} (expected {expected_lower}) - {description}")
    
    print(f"\nLower tier finding: {'PASSED' if all_passed else 'FAILED'}")
    return all_passed

def test_usage_calculations():
    """Test usage flag calculations with mock data"""
    print("\n" + "=" * 60)
    print("TESTING: Usage Flag Calculations")
    print("=" * 60)
    
    collector = AtlasMetadataCollector("test_key", "test_secret", "test_org")
    tier_specs = collector.load_tier_specs()
    
    # Create mock metadata for M30 tier (2 CPU, 8GB RAM, 3000 connections, 3000 IOPS)
    mock_metadata = {
        "tier": "M30",
        "cpu_avg_percent": 25.0,    # 25% of capacity - should be low usage (< 33%)
        "memory_avg_gb": 2.0,       # 2GB of 8GB = 25% - should be low usage (< 33%)
        "iops_avg": 900.0,          # 900 of 3000 = 30% - should be low usage (< 33%)
        "connections_avg": 1200.0,  # 1200 of 3000 = 40% - should NOT be low usage (> 33%)
        "disk_usage_max_gb": 5.0,
        "disk_size_gb": 20.0,       # 5GB of 20GB = 25% - should be low usage (< 33%)
    }
    
    result_metadata = collector.calculate_usage_flags(mock_metadata, tier_specs)
    
    print("M30 tier usage analysis:")
    print(f"  CPU: {result_metadata.get('cpu_avg_percent')}% usage -> low_cpu_use: {result_metadata.get('low_cpu_use')}")
    print(f"  Memory: {result_metadata.get('memory_avg_gb')}GB/{result_metadata.get('memory_tier_limit_gb')}GB -> low_memory_use: {result_metadata.get('low_memory_use')}")
    print(f"  IOPS: {result_metadata.get('iops_avg')}/{result_metadata.get('iops_tier_limit')} -> low_iops_use: {result_metadata.get('low_iops_use')}")
    print(f"  Connections: {result_metadata.get('connections_avg')}/{result_metadata.get('connections_tier_limit')} -> low_connections_use: {result_metadata.get('low_connections_use')}")
    print(f"  Disk: {result_metadata.get('disk_usage_max_gb')}GB/{result_metadata.get('disk_tier_limit_gb')}GB -> low_disk_use: {result_metadata.get('low_disk_use')}")
    
    print("\nLower tier (M20) analysis:")
    print(f"  CPU lower limit: {result_metadata.get('cpu_lower_tier_limit')} cores -> acceptable: {result_metadata.get('cpu_lower_tier_acceptable_use')}")
    print(f"  Memory lower limit: {result_metadata.get('memory_lower_tier_limit_gb')}GB -> acceptable: {result_metadata.get('memory_lower_tier_acceptable_use')}")
    print(f"  IOPS lower limit: {result_metadata.get('iops_lower_tier_limit')} -> acceptable: {result_metadata.get('iops_lower_tier_acceptable_use')}")
    print(f"  Connections lower limit: {result_metadata.get('connections_lower_tier_limit')} -> acceptable: {result_metadata.get('connections_lower_tier_acceptable_use')}")
    
    # Verify expected results based on tier_limits.csv (33% thresholds)
    expected_results = {
        'low_cpu_use': True,      # 25% < 33%
        'low_memory_use': True,   # 25% < 33%
        'low_iops_use': True,     # 30% < 33%
        'low_connections_use': None,  # 40% > 33%
        'low_disk_use': True,     # 25% < 33%
    }
    
    all_passed = True
    for flag, expected in expected_results.items():
        actual = result_metadata.get(flag)
        if actual != expected:
            print(f"  ✗ {flag}: got {actual}, expected {expected}")
            all_passed = False
        else:
            print(f"  ✓ {flag}: {actual}")
    
    print(f"\nUsage calculations: {'PASSED' if all_passed else 'FAILED'}")
    return all_passed

def test_metric_stats_with_time_filter():
    """Test metric statistics calculation with time filtering"""
    print("\n" + "=" * 60)
    print("TESTING: Metric Statistics with Time Filter")
    print("=" * 60)
    
    # Create mock measurement data
    mock_measurement = {
        "dataPoints": [
            {"timestamp": "2023-10-01T10:00:00Z", "value": 50.0},  # Outside filter
            {"timestamp": "2023-10-01T14:00:00Z", "value": 30.0},  # Inside filter
            {"timestamp": "2023-10-01T16:00:00Z", "value": 40.0},  # Inside filter
            {"timestamp": "2023-10-01T18:00:00Z", "value": 60.0},  # Inside filter
            {"timestamp": "2023-10-01T02:00:00Z", "value": 20.0},  # Outside filter
        ]
    }
    
    # Test with time filter (14:00-23:59)
    collector_filtered = AtlasMetadataCollector(
        "test_key", "test_secret", "test_org", 
        time_filter=("14:00", "23:59")
    )
    
    filtered_stats = collector_filtered.calculate_metric_stats_from_single(mock_measurement)
    
    # Test without time filter
    collector_no_filter = AtlasMetadataCollector("test_key", "test_secret", "test_org")
    unfiltered_stats = collector_no_filter.calculate_metric_stats_from_single(mock_measurement)
    
    print("Filtered stats (14:00-23:59 - should use values 30, 40, 60):")
    print(f"  Max: {filtered_stats['max']} (expected 60)")
    print(f"  Avg: {filtered_stats['avg']} (expected ~43.33)")
    print(f"  Count: {filtered_stats['data_point_count']} (expected 3)")
    
    print("\nUnfiltered stats (should use all values 50, 30, 40, 60, 20):")
    print(f"  Max: {unfiltered_stats['max']} (expected 60)")
    print(f"  Avg: {unfiltered_stats['avg']} (expected 40)")
    print(f"  Count: {unfiltered_stats['data_point_count']} (expected 5)")
    
    # Verify results
    all_passed = True
    if filtered_stats['max'] != 60.0:
        print(f"  ✗ Filtered max incorrect: {filtered_stats['max']}")
        all_passed = False
    if abs(filtered_stats['avg'] - 43.33) > 0.1:
        print(f"  ✗ Filtered avg incorrect: {filtered_stats['avg']}")
        all_passed = False
    if filtered_stats['data_point_count'] != 3:
        print(f"  ✗ Filtered count incorrect: {filtered_stats['data_point_count']}")
        all_passed = False
    if unfiltered_stats['data_point_count'] != 5:
        print(f"  ✗ Unfiltered count incorrect: {unfiltered_stats['data_point_count']}")
        all_passed = False
    
    if all_passed:
        print("  ✓ All metric calculations correct")
    
    print(f"\nMetric stats with time filter: {'PASSED' if all_passed else 'FAILED'}")
    return all_passed

def run_all_tests():
    """Run all tests and provide summary"""
    print("MONGODB ATLAS METADATA COLLECTOR - COMPREHENSIVE TEST SUITE")
    print("=" * 80)
    
    tests = [
        ("Time Filtering", test_time_filtering),
        ("Tier Limits Loading", test_tier_limits_loading), 
        ("Tier Specs Loading", test_tier_specs_loading),
        ("Lower Tier Finding", test_lower_tier_finding),
        ("Usage Calculations", test_usage_calculations),
        ("Metric Stats with Filter", test_metric_stats_with_time_filter),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, "PASSED" if result else "FAILED"))
        except Exception as e:
            print(f"ERROR in {test_name}: {e}")
            results.append((test_name, "ERROR"))
    
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    
    passed = 0
    total = len(results)
    
    for test_name, status in results:
        status_symbol = "✓" if status == "PASSED" else "✗"
        print(f"{status_symbol} {test_name}: {status}")
        if status == "PASSED":
            passed += 1
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests PASSED! The new functionality is working correctly.")
    else:
        print("❌ Some tests FAILED. Please check the implementation.")
    
    return passed == total

if __name__ == "__main__":
    success = run_all_tests()

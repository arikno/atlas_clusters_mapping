#!/usr/bin/env python3
"""
Test script for the new time filtering functionality
"""

from datetime import datetime, time
import sys
sys.path.append('.')

from atlas_metadata_collector import AtlasMetadataCollector

def test_time_filtering():
    # Create a collector with time filter for business hours
    collector = AtlasMetadataCollector(
        "test_key", "test_secret", "test_org", 
        time_filter=("14:00", "23:59")
    )
    
    # Test various timestamps
    test_cases = [
        ("2023-10-01T10:30:00Z", False),  # Before business hours
        ("2023-10-01T14:00:00Z", True),   # Start of business hours
        ("2023-10-01T18:30:00Z", True),   # During business hours
        ("2023-10-01T23:59:00Z", True),   # End of business hours
        ("2023-10-01T02:00:00Z", False),  # After business hours
    ]
    
    print("Testing time filtering logic:")
    for timestamp, expected in test_cases:
        result = collector._is_within_time_filter(timestamp)
        status = "✓" if result == expected else "✗"
        print(f"{status} {timestamp} -> {result} (expected {expected})")

def test_tier_limits_loading():
    # Test loading tier limits from file
    collector = AtlasMetadataCollector(
        "test_key", "test_secret", "test_org",
        tier_limits_file="tier_limits.csv"
    )
    
    limits = collector.load_tier_limits()
    print("\nLoaded tier limits:")
    for metric, threshold in limits.items():
        print(f"  {metric}: {threshold}%")

if __name__ == "__main__":
    test_time_filtering()
    test_tier_limits_loading()

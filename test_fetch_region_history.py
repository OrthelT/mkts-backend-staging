#!/usr/bin/env python3
"""
Test script for fetch_region_history functionality.
This script tests the fetch_region_history function with the full nakah watchlist.
"""

import sys
import time
from datetime import datetime, timezone
from dbhandler import get_nakah_watchlist
from nakah import fetch_region_history, fetch_region_item_history
from proj_config import reg_id

def test_fetch_region_history_with_watchlist():
    """Test fetch_region_history with the full nakah watchlist"""
    
    print("=" * 80)
    print("Testing fetch_region_history with full nakah watchlist")
    print("=" * 80)
    
    # Get the full nakah watchlist
    print("1. Loading nakah watchlist...")
    try:
        watchlist = get_nakah_watchlist()
        if watchlist is None or watchlist.empty:
            print("❌ No nakah watchlist found or watchlist is empty")
            return False
        
        print(f"   ✓ Loaded watchlist with {len(watchlist)} items")
        print(f"   Sample items:")
        for i, row in watchlist.head(5).iterrows():
            print(f"     {row['type_id']}: {row['type_name']}")
        
        if len(watchlist) > 5:
            print(f"     ... and {len(watchlist) - 5} more items")
        
    except Exception as e:
        print(f"❌ Error loading watchlist: {e}")
        return False
    
    # Extract type IDs
    type_ids = watchlist['type_id'].tolist()
    print(f"\n2. Extracted {len(type_ids)} type IDs for testing")
    
    # Test with a small subset first (first 5 items)
    print(f"\n3. Testing with first 5 items (subset)...")
    subset_type_ids = type_ids[:5]
    
    start_time = time.time()
    try:
        subset_history = fetch_region_history(reg_id, subset_type_ids)
        subset_time = time.time() - start_time
        
        print(f"   ✓ Subset test completed in {subset_time:.2f} seconds")
        print(f"   ✓ Retrieved history for {len(subset_history)} items")
        
        # Show sample of the returned data
        print(f"   Sample history data:")
        for i, item in enumerate(subset_history[:3]):
            type_id = list(item.keys())[0]
            history_data = list(item.values())[0]
            print(f"     type_id {type_id}: {len(history_data)} history records")
            if history_data:
                sample_record = history_data[0]
                print(f"       Sample record: {list(sample_record.keys())}")
        
    except Exception as e:
        print(f"❌ Error in subset test: {e}")
        return False
    
    # Test with full watchlist (with progress tracking)
    print(f"\n4. Testing with full watchlist ({len(type_ids)} items)...")
    print("   This may take a while depending on the size of your watchlist...")
    
    start_time = time.time()
    try:
        full_history = fetch_region_history(reg_id, type_ids)
        full_time = time.time() - start_time
        
        print(f"   ✓ Full test completed in {full_time:.2f} seconds")
        print(f"   ✓ Retrieved history for {len(full_history)} items")
        
        # Analyze the results
        total_records = 0
        items_with_history = 0
        items_without_history = 0
        
        for item in full_history:
            type_id = list(item.keys())[0]
            history_data = list(item.values())[0]
            
            if history_data and len(history_data) > 0:
                items_with_history += 1
                total_records += len(history_data)
            else:
                items_without_history += 1
        
        print(f"\n5. Analysis:")
        print(f"   Items with history data: {items_with_history}")
        print(f"   Items without history data: {items_without_history}")
        print(f"   Total history records: {total_records}")
        print(f"   Average records per item: {total_records / len(type_ids):.1f}")
        
        # Show some examples of items with and without history
        print(f"\n6. Examples:")
        
        # Items with history
        items_with_data = [item for item in full_history if list(item.values())[0]]
        if items_with_data:
            sample_with_data = items_with_data[0]
            type_id = list(sample_with_data.keys())[0]
            history_data = list(sample_with_data.values())[0]
            print(f"   Item with history (type_id {type_id}):")
            print(f"     Records: {len(history_data)}")
            if history_data:
                sample_record = history_data[0]
                print(f"     Sample record: {sample_record}")
        
        # Items without history
        items_without_data = [item for item in full_history if not list(item.values())[0]]
        if items_without_data:
            sample_without_data = items_without_data[0]
            type_id = list(sample_without_data.keys())[0]
            print(f"   Item without history (type_id {type_id}): No data returned")
        
        return True
        
    except Exception as e:
        print(f"❌ Error in full test: {e}")
        return False

def test_individual_fetch_region_item_history():
    """Test individual fetch_region_item_history function"""
    
    print("\n" + "=" * 80)
    print("Testing individual fetch_region_item_history function")
    print("=" * 80)
    
    # Test with a few well-known type IDs
    test_type_ids = [34, 35, 36, 37, 38, 39]  # Common minerals
    
    for type_id in test_type_ids:
        print(f"\nTesting type_id {type_id}...")
        try:
            start_time = time.time()
            history_data = fetch_region_item_history(reg_id, type_id)
            elapsed_time = time.time() - start_time
            
            if history_data and len(history_data) > 0:
                print(f"   ✓ Success: {len(history_data)} records in {elapsed_time:.2f}s")
                print(f"   Sample record: {history_data[0]}")
            else:
                print(f"   ⚠ No data returned (empty list)")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")

def test_performance_analysis():
    """Analyze performance characteristics"""
    
    print("\n" + "=" * 80)
    print("Performance Analysis")
    print("=" * 80)
    
    # Get a small subset for performance testing
    watchlist = get_nakah_watchlist()
    if watchlist is None or watchlist.empty:
        print("❌ No watchlist available for performance testing")
        return
    
    test_type_ids = watchlist['type_id'].head(10).tolist()
    
    print(f"Testing performance with {len(test_type_ids)} items...")
    
    individual_times = []
    start_time = time.time()
    
    for i, type_id in enumerate(test_type_ids):
        item_start = time.time()
        try:
            history_data = fetch_region_item_history(reg_id, type_id)
            item_time = time.time() - item_start
            individual_times.append(item_time)
            
            print(f"   Item {i+1}/{len(test_type_ids)} (type_id {type_id}): {item_time:.2f}s")
            
        except Exception as e:
            print(f"   Item {i+1}/{len(test_type_ids)} (type_id {type_id}): Error - {e}")
    
    total_time = time.time() - start_time
    
    if individual_times:
        avg_time = sum(individual_times) / len(individual_times)
        min_time = min(individual_times)
        max_time = max(individual_times)
        
        print(f"\nPerformance Summary:")
        print(f"   Total time: {total_time:.2f}s")
        print(f"   Average per item: {avg_time:.2f}s")
        print(f"   Fastest: {min_time:.2f}s")
        print(f"   Slowest: {max_time:.2f}s")
        print(f"   Estimated time for full watchlist: {avg_time * len(watchlist):.1f}s")

if __name__ == "__main__":
    print("fetch_region_history Functionality Test")
    print("This test will:")
    print("1. Load the full nakah watchlist")
    print("2. Test fetch_region_history with a subset")
    print("3. Test fetch_region_history with the full watchlist")
    print("4. Test individual fetch_region_item_history function")
    print("5. Analyze performance characteristics")
    print()
    
    # Test individual function first
    test_individual_fetch_region_item_history()
    
    # Test with watchlist
    success = test_fetch_region_history_with_watchlist()
    
    # Performance analysis
    test_performance_analysis()
    
    if success:
        print("\n🎉 All tests completed successfully!")
        print("The fetch_region_history function is working correctly with your watchlist.")
    else:
        print("\n❌ Some tests failed. Please check the error messages above.")
        sys.exit(1) 
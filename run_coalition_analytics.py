#!/usr/bin/env python3
"""
Winter Coalition Analytics Runner
===============================

Simple script to run all market analytics and intelligence reports.
Perfect for generating Discord-ready content for coalition leadership.

Usage: python run_coalition_analytics.py
"""

import subprocess
import sys
from pathlib import Path

def run_script(script_name, description):
    """Run a Python script and handle errors"""
    print(f"\n🚀 {description}")
    print("=" * 60)
    
    try:
        result = subprocess.run([sys.executable, script_name], 
                              capture_output=False, text=True)
        if result.returncode == 0:
            print(f"✅ {description} completed successfully!")
            return True
        else:
            print(f"❌ {description} failed with return code {result.returncode}")
            return False
    except Exception as e:
        print(f"❌ Error running {script_name}: {e}")
        return False

def main():
    """Run all analytics scripts"""
    
    print("🎯 WINTER COALITION MARKET ANALYTICS SUITE")
    print("=" * 60)
    print("Generating comprehensive market analysis for coalition leadership...")
    print()
    
    scripts = [
        ("coalition_analytics.py", "Standard Market Visualizations & Statistics"),
        ("advanced_market_intel.py", "Strategic Intelligence & Doctrine Analysis")
    ]
    
    success_count = 0
    
    for script, description in scripts:
        if Path(script).exists():
            if run_script(script, description):
                success_count += 1
        else:
            print(f"❌ Script {script} not found!")
    
    print(f"\n🏁 ANALYTICS SUITE COMPLETE")
    print("=" * 60)
    print(f"✅ {success_count}/{len(scripts)} analytics modules completed successfully")
    
    # Show output directories
    output_dirs = [
        "coalition_analytics_output",
        "advanced_intel_output"
    ]
    
    print(f"\n📁 OUTPUT DIRECTORIES:")
    for dir_name in output_dirs:
        if Path(dir_name).exists():
            files = list(Path(dir_name).glob("*"))
            print(f"   • {dir_name}/ ({len(files)} files)")
            for file in files:
                print(f"     - {file.name}")
    
    print(f"\n💬 DISCORD READY FILES:")
    discord_files = [
        "coalition_analytics_output/discord_summary.txt",
        "advanced_intel_output/discord_intel_summary.txt"
    ]
    
    for file_path in discord_files:
        if Path(file_path).exists():
            print(f"   • {file_path}")
    
    print(f"\n🖼️ VISUALIZATION FILES:")
    viz_files = [
        "coalition_analytics_output/market_category_breakdown.png",
        "coalition_analytics_output/top_ships_analysis.png",
        "coalition_analytics_output/top_market_groups.png", 
        "coalition_analytics_output/market_trends_analysis.png"
    ]
    
    for file_path in viz_files:
        if Path(file_path).exists():
            print(f"   • {file_path}")
    
    print(f"\n🎯 NEXT STEPS:")
    print("   1. Share Discord summaries in coalition channels")
    print("   2. Upload visualization PNGs to Discord for context")
    print("   3. Review strategic intelligence report for planning")
    print("   4. Monitor supply alerts and doctrine readiness")
    
    print(f"\n✨ All analytics complete! Ready for coalition leadership review.")

if __name__ == "__main__":
    main()
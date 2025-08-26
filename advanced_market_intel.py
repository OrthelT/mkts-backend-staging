#!/usr/bin/env python3
"""
Advanced Winter Coalition Market Intelligence
===========================================

Provides deep market analysis and intelligence reports for strategic decision making.
Focuses on doctrine readiness, market efficiency, and trading opportunities.

Usage: python advanced_market_intel.py
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from proj_config import deployment_reg_id
from analysis import aggregate_region_history, aggregate_region_history_by_ship
from pathlib import Path
from config import ESIConfig, DatabaseConfig

region_id = deployment_reg_id

def analyze_doctrine_readiness():
    """Analyze market readiness for common fleet doctrines based on current market orders"""

    # Get current market orders (sell orders only)
    engine = create_engine(DatabaseConfig("wcmkt2").engine)
    with engine.connect() as conn:
        stmt = text("""
        SELECT type_id, SUM(volume_remain) as available_volume
        FROM region_orders
        WHERE is_buy_order = 0  -- Only sell orders (ships for sale)
        GROUP BY type_id
        """)
        result = conn.execute(stmt)
        orders_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    # Get ship type information
    sde_engine = create_engine(DatabaseConfig("sde").engine)
    with sde_engine.connect() as conn:
        stmt = text("SELECT typeID, typeName, categoryName FROM inv_info WHERE categoryName = 'Ship'")
        result = conn.execute(stmt)
        ships_info = pd.DataFrame(result.fetchall(), columns=result.keys())

    # Merge to get ship names and filter for ships only
    ships_df = orders_df.merge(ships_info, left_on='type_id', right_on='typeID', how='inner')

    # Define doctrine compositions (simplified)
    doctrines = {
        "Hurricane Fleet Doctrine": {
            "Hurricane Fleet Issue": 40,  # Main DPS
            "Scimitar": 8,               # Logistics
            "Claymore": 2,               # Command Ships
        },
        "Nightmare Doctrine": {
            "Nightmare": 35,             # Main DPS
            "Guardian": 8,               # Logistics
            "Damnation": 2,              # Command Ships
        },
        "Assault Frigate Doctrine": {
            "Retribution": 30,           # Assault Frigs
            "Scimitar": 6,               # Logistics
        }
    }

    doctrine_readiness = {}

    for doctrine_name, composition in doctrines.items():
        readiness_data = {}
        total_fleets_possible = float('inf')

        for ship_name, required_count in composition.items():
            ship_data = ships_df[ships_df['typeName'] == ship_name]

            if len(ship_data) > 0:
                available = int(ship_data.iloc[0]['available_volume'])
                fleets_possible = available // required_count
                total_fleets_possible = min(total_fleets_possible, fleets_possible)

                readiness_data[ship_name] = {
                    'available': available,
                    'required': required_count,
                    'fleets_possible': fleets_possible,
                    'status': 'GOOD' if fleets_possible >= 3 else 'LOW' if fleets_possible >= 1 else 'CRITICAL'
                }
            else:
                readiness_data[ship_name] = {
                    'available': 0,
                    'required': required_count,
                    'fleets_possible': 0,
                    'status': 'UNAVAILABLE'
                }
                total_fleets_possible = 0

        if total_fleets_possible == float('inf'):
            total_fleets_possible = 0

        doctrine_readiness[doctrine_name] = {
            'total_fleets_possible': int(total_fleets_possible),
            'ships': readiness_data,
            'overall_status': 'READY' if total_fleets_possible >= 3 else 'LIMITED' if total_fleets_possible >= 1 else 'NOT_READY'
        }

    return doctrine_readiness

def analyze_market_efficiency():
    """Analyze market efficiency and identify opportunities"""
    engine = create_engine(DatabaseConfig("wcmkt2").engine)

    with engine.connect() as conn:
        # Get recent price volatility
        stmt = text("""
        SELECT type_id,
               AVG(average) as avg_price,
               MIN(average) as min_price,
               MAX(average) as max_price,
               COUNT(*) as data_points,
               (MAX(average) - MIN(average)) / AVG(average) * 100 as volatility_pct
        FROM region_history
        WHERE date >= date('now', '-7 days')
        GROUP BY type_id
        HAVING COUNT(*) >= 3
        ORDER BY volatility_pct DESC
        """)
        result = conn.execute(stmt)
        volatility_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    # Merge with item names
    sde_engine = create_engine(DatabaseConfig("sde").engine)
    with sde_engine.connect() as conn:
        stmt = text("SELECT typeID, typeName, groupName, categoryName FROM inv_info")
        result = conn.execute(stmt)
        items_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    market_efficiency = volatility_df.merge(items_df, left_on='type_id', right_on='typeID', how='left')

    # Identify opportunities
    high_volatility = market_efficiency[market_efficiency['volatility_pct'] > 20].head(10)
    stable_items = market_efficiency[market_efficiency['volatility_pct'] < 5].head(10)

    return {
        'high_volatility_items': high_volatility,
        'stable_items': stable_items,
        'avg_market_volatility': market_efficiency['volatility_pct'].mean()
    }

def analyze_supply_chain_risks():
    """Identify potential supply chain risks based on trading patterns"""
    engine = create_engine(DatabaseConfig("wcmkt2").engine)

    with engine.connect() as conn:
        # Items with declining supply
        stmt = text("""
        WITH recent_supply AS (
            SELECT type_id, AVG(volume) as recent_volume
            FROM region_history
            WHERE date >= date('now', '-7 days')
            GROUP BY type_id
        ),
        historical_supply AS (
            SELECT type_id, AVG(volume) as historical_volume
            FROM region_history
            WHERE date BETWEEN date('now', '-21 days') AND date('now', '-14 days')
            GROUP BY type_id
        )
        SELECT r.type_id,
               r.recent_volume,
               h.historical_volume,
               (r.recent_volume - h.historical_volume) / h.historical_volume * 100 as supply_change_pct
        FROM recent_supply r
        JOIN historical_supply h ON r.type_id = h.type_id
        WHERE h.historical_volume > 0
        ORDER BY supply_change_pct ASC
        """)
        result = conn.execute(stmt)
        supply_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    # Merge with item names
    sde_engine = create_engine(DatabaseConfig("sde").engine)
    with sde_engine.connect() as conn:
        stmt = text("SELECT typeID, typeName, groupName, categoryName FROM inv_info")
        result = conn.execute(stmt)
        items_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    supply_risks = supply_df.merge(items_df, left_on='type_id', right_on='typeID', how='left')

    # Identify critical shortages (>30% decline in supply)
    critical_shortages = supply_risks[supply_risks['supply_change_pct'] < -30].head(15)
    supply_increases = supply_risks[supply_risks['supply_change_pct'] > 50].head(10)

    return {
        'critical_shortages': critical_shortages,
        'supply_increases': supply_increases,
        'avg_supply_change': supply_risks['supply_change_pct'].mean()
    }

def generate_strategic_report():
    """Generate comprehensive strategic market intelligence report"""

    print("📊 Generating Advanced Market Intelligence Report...")

    # Get analysis data
    doctrine_analysis = analyze_doctrine_readiness()
    efficiency_analysis = analyze_market_efficiency()
    supply_analysis = analyze_supply_chain_risks()

    # Create output directory
    output_dir = Path("advanced_intel_output")
    output_dir.mkdir(exist_ok=True)

    # Generate detailed report
    report = f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    WINTER COALITION MARKET INTELLIGENCE REPORT              ║
║                              {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

🎯 DOCTRINE READINESS ASSESSMENT
{'='*80}

"""

    for doctrine_name, data in doctrine_analysis.items():
        status_emoji = {
            'READY': '🟢',
            'LIMITED': '🟡',
            'NOT_READY': '🔴'
        }

        report += f"{status_emoji[data['overall_status']]} {doctrine_name.upper()}\n"
        report += f"   Fleet Capacity: {data['total_fleets_possible']} full fleets\n"
        report += f"   Status: {data['overall_status']}\n"

        for ship_name, ship_data in data['ships'].items():
            status_emoji_ship = {
                'GOOD': '✅',
                'LOW': '⚠️',
                'CRITICAL': '❌',
                'UNAVAILABLE': '❌'
            }
            report += f"   {status_emoji_ship[ship_data['status']]} {ship_name}: {ship_data['available']}/{ship_data['required']} (need {ship_data['required']})\n"

        report += "\n"

    report += f"""
📈 MARKET EFFICIENCY ANALYSIS
{'='*80}

Average Market Volatility: {efficiency_analysis['avg_market_volatility']:.1f}%

🔥 HIGH VOLATILITY OPPORTUNITIES (>20% price swings):
"""

    for idx, item in efficiency_analysis['high_volatility_items'].iterrows():
        report += f"   • {item['typeName']}: {item['volatility_pct']:.1f}% volatility\n"
        report += f"     Price Range: {item['min_price']:,.0f} - {item['max_price']:,.0f} ISK\n"

    report += f"""

🛡️ STABLE MARKETS (<5% volatility):
"""

    for idx, item in efficiency_analysis['stable_items'].head(5).iterrows():
        report += f"   • {item['typeName']}: {item['volatility_pct']:.1f}% volatility\n"

    report += f"""

⚠️ SUPPLY CHAIN RISK ANALYSIS
{'='*80}

Average Supply Change: {supply_analysis['avg_supply_change']:.1f}%

🚨 CRITICAL SUPPLY SHORTAGES (>30% decline):
"""

    for idx, item in supply_analysis['critical_shortages'].iterrows():
        report += f"   • {item['typeName']}: {item['supply_change_pct']:.1f}% supply decline\n"
        report += f"     Category: {item['categoryName']} | Group: {item['groupName']}\n"

    report += f"""

📊 SUPPLY INCREASES (>50% growth):
"""

    for idx, item in supply_analysis['supply_increases'].head(5).iterrows():
        report += f"   • {item['typeName']}: {item['supply_change_pct']:.1f}% supply increase\n"

    report += f"""

🎯 STRATEGIC RECOMMENDATIONS
{'='*80}

IMMEDIATE ACTIONS:
• Monitor Hurricane Fleet Issue supply - it's your primary doctrine backbone
• Consider stockpiling logistics ships if Scimitar supply drops
• Watch for Nightmare price opportunities during high volatility periods

MEDIUM TERM:
• Diversify doctrine options based on market availability
• Establish strategic reserves for critical shortage items
• Monitor competitor stocking patterns in adjacent systems

LONG TERM:
• Consider establishing supply contracts with reliable manufacturers
• Develop alternative doctrines using more readily available ships
• Implement automated market monitoring for early warning signals

Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}
Data source: Regional market history from July 5, 2025 onwards
"""

    # Save report
    with open(output_dir / 'strategic_intelligence_report.txt', 'w', encoding='utf-8') as f:
        f.write(report)

    print(f"📁 Strategic report saved to: {output_dir.absolute()}/strategic_intelligence_report.txt")

    # Create Discord-friendly summary
    discord_intel = f"""
🔍 **WINTER COALITION MARKET INTEL** 🔍

🎯 **DOCTRINE STATUS**
"""

    for doctrine_name, data in doctrine_analysis.items():
        status_emoji = {'READY': '🟢', 'LIMITED': '🟡', 'NOT_READY': '🔴'}
        discord_intel += f"{status_emoji[data['overall_status']]} **{doctrine_name}**: {data['total_fleets_possible']} fleets ready\n"

    discord_intel += f"""
⚠️ **SUPPLY ALERTS**
• {len(supply_analysis['critical_shortages'])} items with critical supply shortages
• Market volatility: {efficiency_analysis['avg_market_volatility']:.1f}% average
• Supply trend: {supply_analysis['avg_supply_change']:+.1f}% change

💡 **KEY INSIGHTS**
• Hurricane Fleet Issues remain market cornerstone
• Logistics ship availability is critical bottleneck
• {len(efficiency_analysis['high_volatility_items'])} high-opportunity trading items identified

*Full intelligence report available for strategic planning*
"""

    # Save Discord summary
    with open(output_dir / 'discord_intel_summary.txt', 'w', encoding='utf-8') as f:
        f.write(discord_intel)

    print(f"📱 Discord summary saved to: {output_dir.absolute()}/discord_intel_summary.txt")

    return {
        'doctrine_analysis': doctrine_analysis,
        'efficiency_analysis': efficiency_analysis,
        'supply_analysis': supply_analysis
    }

if __name__ == "__main__":
    print("🚀 Starting Advanced Market Intelligence Analysis...")
    results = generate_strategic_report()
    print("\n✅ Advanced intelligence analysis complete!")
    print("\n🎯 Key Findings:")
    print(f"   • {sum(1 for d in results['doctrine_analysis'].values() if d['overall_status'] == 'READY')} doctrines are ready")
    print(f"   • {len(results['supply_analysis']['critical_shortages'])} items have critical supply issues")
    print(f"   • {len(results['efficiency_analysis']['high_volatility_items'])} high-opportunity trading items identified")
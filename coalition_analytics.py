#!/usr/bin/env python3
"""
Winter Coalition Market Analytics Dashboard
==========================================

Generates interesting market data visualizations and statistics suitable for Discord sharing.
Data covers the deployment staging market for Winter Coalition in Eve Online.

Usage: python coalition_analytics.py
"""

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import matplotlib.ticker as ticker
from analysis import (
    aggregate_region_history_by_category,
    aggregate_region_history_by_group,
    aggregate_region_history_by_ship,
    aggregate_region_history_by_type,
    aggregate_region_history,
    aggregate_region_history_by_module
)

# Set up style for Discord-friendly dark theme
plt.style.use('dark_background')
sns.set_palette("viridis")

# Create output directory
output_dir = Path("coalition_analytics_output")
output_dir.mkdir(exist_ok=True)

def format_isk_value(value, short=True):
    """Format ISK values in a readable way"""
    if isinstance(value, str):
        return value

    if value >= 1e12:  # Trillion
        return f"{value/1e12:.1f}T ISK" if short else f"{value/1e12:.2f} Trillion ISK"
    elif value >= 1e9:  # Billion
        return f"{value/1e9:.1f}B ISK" if short else f"{value/1e9:.2f} Billion ISK"
    elif value >= 1e6:  # Million
        return f"{value/1e6:.1f}M ISK" if short else f"{value/1e6:.2f} Million ISK"
    elif value >= 1e3:  # Thousand
        return f"{value/1e3:.1f}K ISK" if short else f"{value/1e3:.2f} Thousand ISK"
    else:
        return f"{value:.0f} ISK"

def create_market_category_pie():
    """Create pie chart of market value by category"""
    df = aggregate_region_history_by_category()

    # Convert string values back to numeric for calculations
    df['total_value_numeric'] = df['total_value'].str.replace('B', '').str.replace('M', '').astype(float)
    df.loc[df['total_value'].str.contains('B'), 'total_value_numeric'] *= 1e9
    df.loc[df['total_value'].str.contains('M'), 'total_value_numeric'] *= 1e6

    # Create figure
    fig, ax = plt.subplots(figsize=(12, 8))

    # Create pie chart with better label positioning
    colors = ['#FF6B35', '#F7931E', '#FFD23F', '#06FFA5', '#118AB2', '#073B4C']
    wedges, texts, autotexts = ax.pie(
        df['total_value_numeric'],
        labels=df['categoryName'],
        autopct='%1.1f%%',
        startangle=90,
        colors=colors[:len(df)],
        textprops={'fontsize': 13, 'weight': 'bold'},
        labeldistance=1.1,  # Move category labels further from center
        pctdistance=0.85    # Move percentage labels closer to center
    )

    # Enhance text visibility
    for autotext in autotexts:
        autotext.set_color('white')
        autotext.set_fontsize(12)
        autotext.set_weight('bold')

    for text in texts:
        text.set_fontsize(13)
        text.set_weight('bold')
        text.set_color('white')

    ax.set_title('Winter Coalition Market Value by Category\n' +
                f'Total Market Value: {format_isk_value(df["total_value_numeric"].sum(), False)}',
                fontsize=18, weight='bold', pad=20)

    plt.tight_layout()
    plt.savefig(output_dir / 'market_category_breakdown.png',
                dpi=300, bbox_inches='tight', facecolor='#2C2F36', edgecolor='none')
    plt.close()

    return df['total_value_numeric'].sum()

def create_top_ships_chart():
    """Create bar chart of top traded ships"""
    df = aggregate_region_history_by_ship()

    # Convert millified values back to numeric
    def parse_value(val_str):
        if isinstance(val_str, str):
            multiplier = 1
            if val_str.endswith('B'):
                multiplier = 1e9
                val_str = val_str[:-1]
            elif val_str.endswith('M'):
                multiplier = 1e6
                val_str = val_str[:-1]
            elif val_str.endswith('K') or val_str.endswith('k'):
                multiplier = 1e3
                val_str = val_str[:-1]
            return float(val_str) * multiplier
        return float(val_str)

    df['total_value_numeric'] = df['total_value'].apply(parse_value)
    df['volume_numeric'] = df['volume'].apply(parse_value)
    df['average_numeric'] = df['average'].apply(parse_value)

    # Calculate totals for all ships (categoryName: Ship, categoryID: 6)
    total_ship_value = df['total_value_numeric'].sum()
    total_ship_quantity = df['volume_numeric'].sum()

    # Get top 15 ships by value
    top_ships = df.head(15).copy()

    # Create figure with two subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 12))

    # Left chart: Total Value
    bars1 = ax1.barh(range(len(top_ships)), top_ships['total_value_numeric'],
                     color='#FF6B35', alpha=0.8)
    ax1.set_yticks(range(len(top_ships)))
    ax1.set_yticklabels(top_ships['typeName'], fontsize=11)
    ax1.set_xlabel('Total Market Value (ISK)', fontsize=14, weight='bold')
    ax1.set_title(f'Top Ships by Total Market Value\nAll Ships Total: {format_isk_value(total_ship_value)} ({int(total_ship_quantity):,} units)',
                  fontsize=16, weight='bold', pad=20)

    # Format x-axis
    ax1.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: format_isk_value(x)))
    ax1.tick_params(axis='x', rotation=45)

    # Add value labels on bars
    for i, bar in enumerate(bars1):
        width = bar.get_width()
        ax1.text(width + width*0.01, bar.get_y() + bar.get_height()/2,
                format_isk_value(width), ha='left', va='center', fontsize=9, weight='bold')

    # Right chart: Volume
    bars2 = ax2.barh(range(len(top_ships)), top_ships['volume_numeric'],
                     color='#06FFA5', alpha=0.8)
    ax2.set_yticks(range(len(top_ships)))
    ax2.set_yticklabels(top_ships['typeName'], fontsize=11)
    ax2.set_xlabel('Volume Traded', fontsize=14, weight='bold')
    ax2.set_title(f'Top Ships by Volume Traded\nAll Ships Total: {int(total_ship_quantity):,} units',
                  fontsize=16, weight='bold', pad=20)

    # Add value labels on bars
    for i, bar in enumerate(bars2):
        width = bar.get_width()
        ax2.text(width + width*0.01, bar.get_y() + bar.get_height()/2,
                f'{int(width):,}', ha='left', va='center', fontsize=9, weight='bold')

    plt.tight_layout()
    plt.savefig(output_dir / 'top_ships_analysis.png',
                dpi=300, bbox_inches='tight', facecolor='#2C2F36', edgecolor='none')
    plt.close()

    return top_ships


def create_top_modules_chart():
    """Create bar chart of top traded modules"""
    df = aggregate_region_history_by_module()

    # Convert millified values back to numeric
    def parse_value(val_str):
        if isinstance(val_str, str):
            multiplier = 1
            if val_str.endswith('B'):
                multiplier = 1e9
                val_str = val_str[:-1]
            elif val_str.endswith('M'):
                multiplier = 1e6
                val_str = val_str[:-1]
            elif val_str.endswith('K') or val_str.endswith('k'):
                multiplier = 1e3
                val_str = val_str[:-1]
            return float(val_str) * multiplier
        return float(val_str)

    df['total_value_numeric'] = df['total_value'].apply(parse_value)
    df['volume_numeric'] = df['volume'].apply(parse_value)
    df['average_numeric'] = df['average'].apply(parse_value)

    # Calculate totals for all ships (categoryName: Ship, categoryID: 6)
    total_ship_value = df['total_value_numeric'].sum()
    total_ship_quantity = df['volume_numeric'].sum()

    # Get top 15 ships by value
    top_modules = df.head(15).copy()

    # Create figure with two subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 12))

    # Left chart: Total Value
    bars1 = ax1.barh(range(len(top_modules)), top_modules['total_value_numeric'],
                     color='#FF6B35', alpha=0.8)
    ax1.set_yticks(range(len(top_modules)))
    ax1.set_yticklabels(top_modules['typeName'], fontsize=11)
    ax1.set_xlabel('Total Market Value (ISK)', fontsize=14, weight='bold')
    ax1.set_title(f'Top Modules by Total Market Value\nAll Modules Total: {format_isk_value(total_ship_value)} ({int(total_ship_quantity):,} units)',
                  fontsize=16, weight='bold', pad=20)

    # Format x-axis
    ax1.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: format_isk_value(x)))
    ax1.tick_params(axis='x', rotation=45)

    # Add value labels on bars
    for i, bar in enumerate(bars1):
        width = bar.get_width()
        ax1.text(width + width*0.01, bar.get_y() + bar.get_height()/2,
                format_isk_value(width), ha='left', va='center', fontsize=9, weight='bold')

    # Right chart: Volume
    bars2 = ax2.barh(range(len(top_modules)), top_modules['volume_numeric'],
                     color='#06FFA5', alpha=0.8)
    ax2.set_yticks(range(len(top_modules)))
    ax2.set_yticklabels(top_modules['typeName'], fontsize=11)
    ax2.set_xlabel('Volume Traded', fontsize=14, weight='bold')
    ax2.set_title(f'Top Modules by Volume Traded\nAll Modules Total: {int(total_ship_quantity):,} units',
                  fontsize=16, weight='bold', pad=20)

    # Add value labels on bars
    for i, bar in enumerate(bars2):
        width = bar.get_width()
        ax2.text(width + width*0.01, bar.get_y() + bar.get_height()/2,
                f'{int(width):,}', ha='left', va='center', fontsize=9, weight='bold')

    plt.tight_layout()
    plt.savefig(output_dir / 'top_modules_analysis.png',
                dpi=300, bbox_inches='tight', facecolor='#2C2F36', edgecolor='none')
    plt.close()

    return top_modules

def create_market_groups_analysis():
    """Create analysis of top market groups"""
    df = aggregate_region_history_by_group()

    # Convert millified values back to numeric
    def parse_value(val_str):
        if isinstance(val_str, str):
            multiplier = 1
            if val_str.endswith('B'):
                multiplier = 1e9
                val_str = val_str[:-1]
            elif val_str.endswith('M'):
                multiplier = 1e6
                val_str = val_str[:-1]
            elif val_str.endswith('K') or val_str.endswith('k'):
                multiplier = 1e3
                val_str = val_str[:-1]
            return float(val_str) * multiplier
        return float(val_str)

    df['total_value_numeric'] = df['total_value'].apply(parse_value)
    df['volume_numeric'] = df['volume'].apply(parse_value)

    # Get top 20 groups
    top_groups = df.head(20)

    # Create figure
    fig, ax = plt.subplots(figsize=(16, 12))

    # Create horizontal bar chart
    bars = ax.barh(range(len(top_groups)), top_groups['total_value_numeric'],
                   color='#118AB2', alpha=0.8)

    ax.set_yticks(range(len(top_groups)))
    ax.set_yticklabels(top_groups['groupName'], fontsize=10)
    ax.set_xlabel('Total Market Value (ISK)', fontsize=14, weight='bold')
    ax.set_title('Top Market Groups by Value\n(Equipment Categories)',
                 fontsize=18, weight='bold', pad=20)

    # Format x-axis
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: format_isk_value(x)))

    # Add value labels on bars
    for i, bar in enumerate(bars):
        width = bar.get_width()
        ax.text(width + width*0.01, bar.get_y() + bar.get_height()/2,
                format_isk_value(width), ha='left', va='center', fontsize=9, weight='bold')

    # Invert y-axis to have highest values at top
    ax.invert_yaxis()

    plt.tight_layout()
    plt.savefig(output_dir / 'top_market_groups.png',
                dpi=300, bbox_inches='tight', facecolor='#2C2F36', edgecolor='none')
    plt.close()

    return top_groups

def create_market_summary_stats():
    """Generate comprehensive market summary statistics"""

    # Get all data
    df_all = aggregate_region_history()
    df_category = aggregate_region_history_by_category()
    df_ships = aggregate_region_history_by_ship()
    df_groups = aggregate_region_history_by_group()

    # Convert category data to numeric
    def parse_category_value(val_str):
        if isinstance(val_str, str):
            multiplier = 1
            if val_str.endswith('B'):
                multiplier = 1e9
                val_str = val_str[:-1]
            elif val_str.endswith('M'):
                multiplier = 1e6
                val_str = val_str[:-1]
            elif val_str.endswith('K'):
                multiplier = 1e3
                val_str = val_str[:-1]
            return float(val_str) * multiplier
        return float(val_str)

    df_category['total_value_numeric'] = df_category['total_value'].apply(parse_category_value)
    total_market_value = df_category['total_value_numeric'].sum()

    # Calculate statistics
    stats = {
        'total_market_value': total_market_value,
        'unique_items_traded': len(df_all),
        'total_categories': len(df_category),
        'ship_types_available': len(df_ships),
        'top_item': df_all.iloc[0]['typeName'] if len(df_all) > 0 else "N/A",
        'top_item_value': df_all.iloc[0]['total_value'] if len(df_all) > 0 else 0,
        'ship_market_value': df_category[df_category['categoryName'] == 'Ship']['total_value_numeric'].iloc[0] if 'Ship' in df_category['categoryName'].values else 0,
        'module_market_value': df_category[df_category['categoryName'] == 'Module']['total_value_numeric'].iloc[0] if 'Module' in df_category['categoryName'].values else 0,
    }

    return stats

def generate_discord_summary_text(stats, top_ships, top_modules):
    """Generate Discord-friendly text summary"""

    summary = f"""
🚀 **WINTER COALITION MARKET REPORT** 🚀
*Deployment Staging Market Analysis*

📊 **MARKET OVERVIEW**
• **Total Market Value**: {format_isk_value(stats['total_market_value'], False)}
• **Unique Items Traded**: {stats['unique_items_traded']:,}
• **Ship Types Available**: {stats['ship_types_available']:,}

⭐ **TOP ITEMS (by total value traded)**
• **TOP ITEM**: {stats['top_item']}
• **Ships Market Value**: {format_isk_value(stats['ship_market_value'], False)}
• **Modules Market Value**: {format_isk_value(stats['module_market_value'], False)}

🛸 **TOP 5 SHIPS BY VALUE**
"""

    for i, ship in top_ships.head(5).iterrows():
        ship_value = ship['total_value_numeric']
        volume = int(ship['volume_numeric'])
        summary += f"• **{ship['typeName']}**: {format_isk_value(ship_value)} ({volume:,} units)\n"

    summary += f"""

🛸 **TOP 5 MODULES BY VALUE**
"""

    for i, module in top_modules.head(5).iterrows():
        module_value = module['total_value_numeric']
        volume = int(module['volume_numeric'])
        summary += f"• **{module['typeName']}**: {format_isk_value(module_value)} ({volume:,} units)\n"

    summary += f"""

*Data covers period from July 5, 2025 onwards*
*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}*
    """

    return summary

def create_market_trends_chart():
    """Create time-series chart showing market activity trends"""
    from sqlalchemy import create_engine, text
    from proj_config import wcmkt_local_url

    engine = create_engine(wcmkt_local_url)
    with engine.connect() as conn:
        stmt = text("""
        SELECT date,
               COUNT(*) as items_traded,
               SUM(average * volume) as daily_value,
               AVG(average) as avg_price,
               SUM(volume) as total_volume
        FROM region_history
        WHERE date > '2025-07-05'
        GROUP BY date
        ORDER BY date ASC
        """)
        result = conn.execute(stmt)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

    # Convert date column
    df['date'] = pd.to_datetime(df['date'])

    # Create figure with subplots
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 12))

    # Daily market value trend
    ax1.plot(df['date'], df['daily_value'], color='#FF6B35', linewidth=3, marker='o', markersize=6)
    ax1.set_title('Daily Market Value', fontsize=14, weight='bold')
    ax1.set_ylabel('ISK Value', fontsize=12)
    ax1.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: format_isk_value(x)))
    ax1.tick_params(axis='x', rotation=45)
    ax1.grid(True, alpha=0.3)

    # Items traded per day
    ax2.bar(df['date'], df['items_traded'], color='#06FFA5', alpha=0.8)
    ax2.set_title('Unique Items Traded Daily', fontsize=14, weight='bold')
    ax2.set_ylabel('Number of Items', fontsize=12)
    ax2.tick_params(axis='x', rotation=45)
    ax2.grid(True, alpha=0.3)

    # Volume trends
    ax3.plot(df['date'], df['total_volume'], color='#118AB2', linewidth=3, marker='s', markersize=6)
    ax3.set_title('Daily Trading Volume', fontsize=14, weight='bold')
    ax3.set_ylabel('Volume', fontsize=12)
    ax3.tick_params(axis='x', rotation=45)
    ax3.grid(True, alpha=0.3)

    # Average price trends
    ax4.plot(df['date'], df['avg_price'], color='#F7931E', linewidth=3, marker='^', markersize=6)
    ax4.set_title('Average Item Price Trend', fontsize=14, weight='bold')
    ax4.set_ylabel('Average Price (ISK)', fontsize=12)
    ax4.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: format_isk_value(x)))
    ax4.tick_params(axis='x', rotation=45)
    ax4.grid(True, alpha=0.3)

    # Overall title
    fig.suptitle('Winter Coalition Market Activity Trends\n30-Day Historical Analysis',
                 fontsize=18, weight='bold', y=0.98)

    plt.tight_layout()
    plt.subplots_adjust(top=0.92)
    plt.savefig(output_dir / 'market_trends_analysis.png',
                dpi=300, bbox_inches='tight', facecolor='#2C2F36', edgecolor='none')
    plt.close()

    # Calculate trend statistics
    recent_avg = df.tail(7)['daily_value'].mean()
    overall_avg = df['daily_value'].mean()
    trend_pct = ((recent_avg - overall_avg) / overall_avg) * 100

    return {
        'recent_avg_daily': recent_avg,
        'overall_avg_daily': overall_avg,
        'trend_percentage': trend_pct,
        'total_days': len(df),
        'peak_day_value': df['daily_value'].max(),
        'peak_day_date': df.loc[df['daily_value'].idxmax(), 'date'].strftime('%Y-%m-%d')
    }

def main():
    """Generate all analytics"""
    print("🚀 Generating Winter Coalition Market Analytics...")

    # Create visualizations
    print("📊 Creating market category breakdown...")
    total_value = create_market_category_pie()

    print("🛸 Analyzing top ships...")
    top_ships = create_top_ships_chart()

    print("🛸 Analyzing top modules...")
    top_modules = create_top_modules_chart()

    print("⚙️ Creating market groups analysis...")
    top_groups = create_market_groups_analysis()

    print("📈 Creating market trends analysis...")
    trend_stats = create_market_trends_chart()

    print("📊 Generating summary statistics...")
    stats = create_market_summary_stats()

    print("📝 Creating Discord summary...")
    summary_text = generate_discord_summary_text(stats, top_ships, top_modules)

    # Save summary text
    with open(output_dir / 'discord_summary.txt', 'w') as f:
        f.write(summary_text)

    print(f"\n✅ Analytics complete! Files saved to: {output_dir.absolute()}")
    print(f"📁 Generated files:")
    print(f"   • market_category_breakdown.png")
    print(f"   • top_ships_analysis.png")
    print(f"   • top_modules_analysis.png")
    print(f"   • top_market_groups.png")
    print(f"   • market_trends_analysis.png")
    print(f"   • discord_summary.txt")

    print(f"\n📊 Quick Stats:")
    print(f"   • Total Market Value: {format_isk_value(stats['total_market_value'], False)}")
    print(f"   • Unique Items: {stats['unique_items_traded']:,}")
    print(f"   • Ship Types: {stats['ship_types_available']:,}")

    print(f"\n📈 Market Trends ({trend_stats['total_days']} days):")
    print(f"   • Peak Day Value: {format_isk_value(trend_stats['peak_day_value'], False)} on {trend_stats['peak_day_date']}")
    print(f"   • Recent 7-day Avg: {format_isk_value(trend_stats['recent_avg_daily'], False)}")
    print(f"   • Overall Daily Avg: {format_isk_value(trend_stats['overall_avg_daily'], False)}")
    trend_direction = "📈 UP" if trend_stats['trend_percentage'] > 0 else "📉 DOWN"
    print(f"   • Recent Trend: {trend_direction} {abs(trend_stats['trend_percentage']):.1f}%")

    # Print Discord summary for immediate use
    print(f"\n📝 DISCORD SUMMARY:")
    print("=" * 60)
    print(summary_text)

if __name__ == "__main__":
    main()
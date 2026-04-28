"""
Gold layer aggregations - backward compatible exports.

This module now delegates to the generic gold processor.
Add new gold tables by editing gold/config.py instead of writing new functions.
"""
from gold.generic import process_gold_table


def aggregate_class_daily_performance(incremental: bool = True, full_refresh: bool = False):
    """
    Aggregates silver fact tables to gold class_daily_performance.

    This is a convenience function for backward compatibility.
    The actual logic is config-driven in gold/config.py.

    Args:
        incremental: If True, only process new/changed dates since last run
        full_refresh: If True, ignore incremental and process all data
    """
    process_gold_table("class_daily_performance", incremental=incremental, full_refresh=full_refresh)

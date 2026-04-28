"""
Silver layer transformation functions.

This module provides backward-compatible wrapper functions that delegate
to the new config-driven fact builder in fact_builder.py.

For production use, fact tables are now defined in silver/fact_config.py
and built using the generic fact_builder.build_fact_table_generic() function.
"""

from silver.fact_builder import build_fact_student_performance, build_fact_class_summary

# Re-export for backward compatibility
__all__ = ["build_fact_student_performance", "build_fact_class_summary"]

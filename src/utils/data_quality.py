import polars as pl
import logging
from typing import Dict, List, Optional, Callable
from utils.audit import log_execution

logger = logging.getLogger(__name__)


class DataQualityCheck:
    """Base class for data quality checks"""

    def __init__(self, name: str, severity: str = "error"):
        """
        Args:
            name: Name of the check
            severity: "error" (fail pipeline), "warning" (log but continue), "info" (just log)
        """
        self.name = name
        self.severity = severity

    def check(self, df: pl.DataFrame) -> tuple[bool, str]:
        """
        Returns: (passed, message)
        """
        raise NotImplementedError


class RowCountCheck(DataQualityCheck):
    """Validates minimum row count"""

    def __init__(self, min_rows: int = 1, severity: str = "error"):
        super().__init__(f"RowCount >= {min_rows}", severity)
        self.min_rows = min_rows

    def check(self, df: pl.DataFrame) -> tuple[bool, str]:
        row_count = len(df)
        passed = row_count >= self.min_rows
        message = f"Row count: {row_count} (min: {self.min_rows})"
        return passed, message


class NullCheck(DataQualityCheck):
    """Validates that specified columns have no nulls"""

    def __init__(self, columns: List[str], severity: str = "error"):
        super().__init__(f"NullCheck: {columns}", severity)
        self.columns = columns

    def check(self, df: pl.DataFrame) -> tuple[bool, str]:
        null_counts = {}
        for col in self.columns:
            if col in df.columns:
                null_count = df[col].null_count()
                if null_count > 0:
                    null_counts[col] = null_count

        passed = len(null_counts) == 0
        if passed:
            message = f"No nulls in {self.columns}"
        else:
            message = f"Nulls found: {null_counts}"
        return passed, message


class UniqueCheck(DataQualityCheck):
    """Validates uniqueness of specified columns"""

    def __init__(self, columns: List[str], severity: str = "error"):
        super().__init__(f"UniqueCheck: {columns}", severity)
        self.columns = columns

    def check(self, df: pl.DataFrame) -> tuple[bool, str]:
        total_rows = len(df)
        unique_rows = df.select(self.columns).unique().height
        passed = total_rows == unique_rows

        if passed:
            message = f"All rows unique on {self.columns}"
        else:
            duplicates = total_rows - unique_rows
            message = f"Found {duplicates} duplicate rows on {self.columns}"
        return passed, message


class ValueRangeCheck(DataQualityCheck):
    """Validates numeric column is within a range"""

    def __init__(self, column: str, min_val: Optional[float] = None,
                 max_val: Optional[float] = None, severity: str = "error"):
        super().__init__(f"RangeCheck: {column}", severity)
        self.column = column
        self.min_val = min_val
        self.max_val = max_val

    def check(self, df: pl.DataFrame) -> tuple[bool, str]:
        if self.column not in df.columns:
            return False, f"Column {self.column} not found"

        violations = 0
        if self.min_val is not None:
            violations += (df[self.column] < self.min_val).sum()
        if self.max_val is not None:
            violations += (df[self.column] > self.max_val).sum()

        passed = violations == 0
        range_str = f"[{self.min_val}, {self.max_val}]"
        message = f"{self.column} in {range_str}: {violations} violations" if not passed else f"{self.column} in {range_str}"
        return passed, message


class CustomCheck(DataQualityCheck):
    """Custom validation using a user-provided function"""

    def __init__(self, name: str, check_fn: Callable[[pl.DataFrame], tuple[bool, str]],
                 severity: str = "error"):
        super().__init__(name, severity)
        self.check_fn = check_fn

    def check(self, df: pl.DataFrame) -> tuple[bool, str]:
        return self.check_fn(df)


class DataQualityRunner:
    """Runs a suite of data quality checks and handles results"""

    def __init__(self, table_name: str, checks: List[DataQualityCheck]):
        self.table_name = table_name
        self.checks = checks

    def run(self, df: pl.DataFrame) -> bool:
        """
        Run all checks and log results.
        Returns: True if all checks passed (or only warnings), False if any error-level check failed
        """
        all_passed = True
        errors = []
        warnings = []
        infos = []

        for check in self.checks:
            try:
                passed, message = check.check(df)

                status_str = "✓" if passed else "✗"
                full_message = f"{status_str} {check.name}: {message}"

                if not passed:
                    if check.severity == "error":
                        errors.append(full_message)
                        all_passed = False
                    elif check.severity == "warning":
                        warnings.append(full_message)
                    else:
                        infos.append(full_message)
                else:
                    logger.info(f"  {full_message}")

            except Exception as e:
                error_msg = f"✗ {check.name}: Check failed with exception: {str(e)}"
                errors.append(error_msg)
                all_passed = False

        # Log summary
        if errors:
            logger.error(f"❌ Data Quality FAILED for {self.table_name}:")
            for err in errors:
                logger.error(f"  {err}")

        if warnings:
            logger.warning(f"⚠️  Data Quality WARNINGS for {self.table_name}:")
            for warn in warnings:
                logger.warning(f"  {warn}")

        if all_passed and not warnings:
            logger.info(f"✅ All data quality checks passed for {self.table_name}")

        # Log to audit table
        status = "success" if all_passed else "failed"
        message = "; ".join(errors + warnings) if (errors or warnings) else "All checks passed"
        log_execution(
            table_name=self.table_name,
            layer_type="data_quality",
            status=status,
            message=message[:500]  # Truncate if too long
        )

        return all_passed


def get_default_dim_checks(table_config: Dict) -> List[DataQualityCheck]:
    """Generate standard data quality checks for dimension tables"""
    checks = [
        RowCountCheck(min_rows=1, severity="error"),
    ]

    # Add null checks for not_null_cols
    if "not_null_cols" in table_config:
        checks.append(NullCheck(table_config["not_null_cols"], severity="error"))

    # Add uniqueness check for dedup_keys
    if "dedup_keys" in table_config:
        checks.append(UniqueCheck(table_config["dedup_keys"], severity="error"))

    return checks


def get_default_fact_checks(primary_keys: List[str]) -> List[DataQualityCheck]:
    """Generate standard data quality checks for fact tables"""
    checks = [
        RowCountCheck(min_rows=1, severity="error"),
        NullCheck(primary_keys, severity="error"),
        UniqueCheck(primary_keys, severity="error"),
    ]
    return checks

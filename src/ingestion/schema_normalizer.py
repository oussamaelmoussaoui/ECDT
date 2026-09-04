"""
ECDT - Phase 2
Schema normalization for telemetry events.

This module converts metrics, logs and traces loaded from RCAEval-derived
datasets into a common normalized event representation.

Responsibilities
----------------
This module is responsible for:

    - converting heterogeneous telemetry schemas into a common schema;
    - extracting service names and signal types from metric names;
    - normalizing timestamps;
    - preserving RCAEval case metadata;
    - producing Polars DataFrames suitable for downstream processing.

This module is NOT responsible for:

    - anomaly detection;
    - root-cause inference;
    - incident classification;
    - topology inference;
    - LLM reasoning.

Canonical timestamp
-------------------
All normalized events use:

    timestamp_ms : Unix epoch milliseconds
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import polars as pl


# ============================================================================
# Exceptions
# ============================================================================


class SchemaNormalizerError(RuntimeError):
    """Base exception for schema normalization errors."""


class NormalizationSchemaError(SchemaNormalizerError):
    """Raised when an input dataframe does not have the expected schema."""


# ============================================================================
# Normalized event model
# ============================================================================


@dataclass(frozen=True)
class NormalizedEvent:
    """
    Common representation of one telemetry event.

    Not every field is populated for every source.

    Examples
    --------
    Metric event:

        source="metric"
        service_name="checkoutservice"
        signal_type="cpu"
        metric_name="checkoutservice_cpu"
        value=0.82

    Log event:

        source="log"
        service_name="frontend"
        message="request started"

    Trace event:

        source="trace"
        service_name="currencyservice"
        operation_name="Convert"
        duration_ms=186
    """

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------

    event_id: str
    case_id: str
    timestamp_ms: int

    # ------------------------------------------------------------------
    # Dataset context
    # ------------------------------------------------------------------

    dataset: Optional[str] = None

    # ------------------------------------------------------------------
    # Source
    # ------------------------------------------------------------------

    source: str = ""

    # ------------------------------------------------------------------
    # Service
    # ------------------------------------------------------------------

    service_name: Optional[str] = None

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------

    signal_type: Optional[str] = None
    metric_name: Optional[str] = None
    value: Optional[float] = None

    # ------------------------------------------------------------------
    # Logs
    # ------------------------------------------------------------------

    message: Optional[str] = None

    # ------------------------------------------------------------------
    # Traces
    # ------------------------------------------------------------------

    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    parent_span_id: Optional[str] = None
    method_name: Optional[str] = None
    operation_name: Optional[str] = None
    duration_ms: Optional[float] = None
    status_code: Optional[float] = None


# ============================================================================
# Normalizer
# ============================================================================


class SchemaNormalizer:
    """
    Normalize metrics, logs and traces into a common telemetry schema.

    The normalizer works on Polars DataFrames produced by DatasetLoader.

    Example
    -------
    >>> normalizer = SchemaNormalizer()

    >>> metrics = normalizer.normalize_metrics(
    ...     loader.load_metrics(
    ...         case_id="re2ob_checkoutservice_cpu_1",
    ...         long_format=True
    ...     )
    ... )

    >>> logs = normalizer.normalize_logs(
    ...     loader.load_logs(
    ...         case_id="re2ob_checkoutservice_cpu_1"
    ...     )
    ... )

    >>> traces = normalizer.normalize_traces(
    ...     loader.load_traces(
    ...         case_id="re2ob_checkoutservice_cpu_1"
    ...     )
    ... )
    """

    # ------------------------------------------------------------------
    # Canonical signal types
    # ------------------------------------------------------------------

    SUPPORTED_SIGNAL_TYPES = {
        "cpu",
        "mem",
        "diskio",
        "socket",
        "workload",
        "error",
        "latency-50",
        "latency-90",
    }

    # ------------------------------------------------------------------
    # Expected source schemas
    # ------------------------------------------------------------------

    REQUIRED_METRIC_COLUMNS = {
        "case",
        "dataset",
        "timestamp",
        "metric_name",
        "value",
    }

    REQUIRED_LOG_COLUMNS = {
        "case",
        "dataset",
        "timestamp_ms",
        "container_name",
        "message",
    }

    REQUIRED_TRACE_COLUMNS = {
        "case",
        "dataset",
        "timestamp_ms",
        "traceID",
        "spanID",
        "serviceName",
        "methodName",
        "operationName",
        "parentSpanID",
        "duration",
        "statusCode",
    }

    # ------------------------------------------------------------------
    # Metric suffixes
    # ------------------------------------------------------------------

    METRIC_SUFFIXES = (
        "latency-50",
        "latency-90",
        "diskio",
        "workload",
        "socket",
        "error",
        "cpu",
        "mem",
    )

    # ------------------------------------------------------------------
    # Constructor
    # ------------------------------------------------------------------

    def __init__(self) -> None:
        pass

    # ==================================================================
    # Public API - metrics
    # ==================================================================

    def normalize_metrics(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        """
        Normalize metric observations.

        Input
        -----
        Expected long-format dataframe:

            case
            dataset
            timestamp
            metric_name
            value

        Output
        ------
        A dataframe using the canonical normalized event schema.
        """

        self._validate_columns(
            df,
            self.REQUIRED_METRIC_COLUMNS,
            "metrics",
        )

        if df.is_empty():
            return self.empty_dataframe()

        normalized = (
            df
            .with_columns(
                [
                    pl.col("case")
                    .cast(pl.Utf8)
                    .alias("case_id"),

                    pl.col("timestamp")
                    .cast(pl.Int64)
                    .alias("timestamp_ms"),

                    pl.col("dataset")
                    .cast(pl.Utf8),

                    pl.col("metric_name")
                    .cast(pl.Utf8),

                    pl.col("value")
                    .cast(pl.Float64, strict=False),
                ]
            )
            .with_columns(
                [
                    self._extract_service_expression(
                        "metric_name"
                    ).alias("service_name"),

                    self._extract_signal_type_expression(
                        "metric_name"
                    ).alias("signal_type"),
                ]
            )
            .with_columns(
                pl.concat_str(
                    [
                        pl.lit("metric:"),
                        pl.col("case_id"),
                        pl.lit(":"),
                        pl.col("timestamp_ms").cast(pl.Utf8),
                        pl.lit(":"),
                        pl.col("metric_name"),
                    ],
                    separator="",
                ).alias("event_id")
            )
            .select(
                [
                    "event_id",
                    "case_id",
                    "timestamp_ms",
                    "dataset",
                    pl.lit("metric").alias("source"),
                    "service_name",
                    "signal_type",
                    "metric_name",
                    "value",
                    pl.lit(None, dtype=pl.Utf8).alias("message"),
                    pl.lit(None, dtype=pl.Utf8).alias("trace_id"),
                    pl.lit(None, dtype=pl.Utf8).alias("span_id"),
                    pl.lit(None, dtype=pl.Utf8).alias("parent_span_id"),
                    pl.lit(None, dtype=pl.Utf8).alias("method_name"),
                    pl.lit(None, dtype=pl.Utf8).alias("operation_name"),
                    pl.lit(None, dtype=pl.Float64).alias("duration_ms"),
                    pl.lit(None, dtype=pl.Float64).alias("status_code"),
                ]
            )
        )

        return self._finalize(normalized)

    # ==================================================================
    # Public API - logs
    # ==================================================================

    def normalize_logs(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        """
        Normalize log events.
        """

        self._validate_columns(
            df,
            self.REQUIRED_LOG_COLUMNS,
            "logs",
        )

        if df.is_empty():
            return self.empty_dataframe()

        normalized = (
            df
            .with_columns(
                [
                    pl.col("case")
                    .cast(pl.Utf8)
                    .alias("case_id"),

                    pl.col("timestamp_ms")
                    .cast(pl.Int64),

                    pl.col("dataset")
                    .cast(pl.Utf8),

                    pl.col("container_name")
                    .cast(pl.Utf8)
                    .alias("service_name"),

                    pl.col("message")
                    .cast(pl.Utf8),
                ]
            )
            .with_columns(
                pl.int_range(
                    0,
                    pl.len(),
                    dtype=pl.Int64,
                ).alias("_row_id")
            )
            .with_columns(
                pl.concat_str(
                    [
                        pl.lit("log:"),
                        pl.col("case_id"),
                        pl.lit(":"),
                        pl.col("timestamp_ms").cast(pl.Utf8),
                        pl.lit(":"),
                        pl.col("_row_id").cast(pl.Utf8),
                    ],
                    separator="",
                ).alias("event_id")
            )
            .select(
                [
                    "event_id",
                    "case_id",
                    "timestamp_ms",
                    "dataset",
                    pl.lit("log").alias("source"),
                    "service_name",
                    pl.lit(None, dtype=pl.Utf8).alias(
                        "signal_type"
                    ),
                    pl.lit(None, dtype=pl.Utf8).alias(
                        "metric_name"
                    ),
                    pl.lit(None, dtype=pl.Float64).alias(
                        "value"
                    ),
                    "message",
                    pl.lit(None, dtype=pl.Utf8).alias(
                        "trace_id"
                    ),
                    pl.lit(None, dtype=pl.Utf8).alias(
                        "span_id"
                    ),
                    pl.lit(None, dtype=pl.Utf8).alias(
                        "parent_span_id"
                    ),
                    pl.lit(None, dtype=pl.Utf8).alias(
                        "method_name"
                    ),
                    pl.lit(None, dtype=pl.Utf8).alias(
                        "operation_name"
                    ),
                    pl.lit(None, dtype=pl.Float64).alias(
                        "duration_ms"
                    ),
                    pl.lit(None, dtype=pl.Float64).alias(
                        "status_code"
                    ),
                ]
            )
        )

        return self._finalize(normalized)

    # ==================================================================
    # Public API - traces
    # ==================================================================

    def normalize_traces(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        """
        Normalize trace/span events.
        """

        self._validate_columns(
            df,
            self.REQUIRED_TRACE_COLUMNS,
            "traces",
        )

        if df.is_empty():
            return self.empty_dataframe()

        normalized = (
            df
            .with_columns(
                [
                    pl.col("case")
                    .cast(pl.Utf8)
                    .alias("case_id"),

                    pl.col("timestamp_ms")
                    .cast(pl.Int64),

                    pl.col("dataset")
                    .cast(pl.Utf8),

                    pl.col("serviceName")
                    .cast(pl.Utf8)
                    .alias("service_name"),

                    pl.col("traceID")
                    .cast(pl.Utf8)
                    .alias("trace_id"),

                    pl.col("spanID")
                    .cast(pl.Utf8)
                    .alias("span_id"),

                    pl.col("parentSpanID")
                    .cast(pl.Utf8)
                    .alias("parent_span_id"),

                    pl.col("methodName")
                    .cast(pl.Utf8)
                    .alias("method_name"),

                    pl.col("operationName")
                    .cast(pl.Utf8)
                    .alias("operation_name"),

                    pl.col("duration")
                    .cast(pl.Float64, strict=False)
                    .alias("duration_ms"),

                    pl.col("statusCode")
                    .cast(pl.Float64, strict=False)
                    .alias("status_code"),
                ]
            )
            .with_columns(
                pl.int_range(
                    0,
                    pl.len(),
                    dtype=pl.Int64,
                ).alias("_row_id")
            )
            .with_columns(
                pl.concat_str(
                    [
                        pl.lit("trace:"),
                        pl.col("case_id"),
                        pl.lit(":"),
                        pl.col("span_id").fill_null("unknown"),
                        pl.lit(":"),
                        pl.col("_row_id").cast(pl.Utf8),
                    ],
                    separator="",
                ).alias("event_id")
            )
            .select(
                [
                    "event_id",
                    "case_id",
                    "timestamp_ms",
                    "dataset",
                    pl.lit("trace").alias("source"),
                    "service_name",
                    pl.lit(None, dtype=pl.Utf8).alias(
                        "signal_type"
                    ),
                    pl.lit(None, dtype=pl.Utf8).alias(
                        "metric_name"
                    ),
                    pl.lit(None, dtype=pl.Float64).alias(
                        "value"
                    ),
                    pl.lit(None, dtype=pl.Utf8).alias(
                        "message"
                    ),
                    "trace_id",
                    "span_id",
                    "parent_span_id",
                    "method_name",
                    "operation_name",
                    "duration_ms",
                    "status_code",
                ]
            )
        )

        return self._finalize(normalized)

    # ==================================================================
    # Public API - all telemetry
    # ==================================================================

    def normalize_all(
        self,
        *,
        metrics: Optional[pl.DataFrame] = None,
        logs: Optional[pl.DataFrame] = None,
        traces: Optional[pl.DataFrame] = None,
    ) -> pl.DataFrame:
        """
        Normalize and combine any supplied telemetry sources.

        Parameters
        ----------
        metrics:
            Metrics dataframe in long format.

        logs:
            Logs dataframe.

        traces:
            Traces dataframe.

        Returns
        -------
        pl.DataFrame
            Unified normalized event dataframe.
        """

        frames: list[pl.DataFrame] = []

        if metrics is not None:
            frames.append(
                self.normalize_metrics(metrics)
            )

        if logs is not None:
            frames.append(
                self.normalize_logs(logs)
            )

        if traces is not None:
            frames.append(
                self.normalize_traces(traces)
            )

        if not frames:
            return self.empty_dataframe()

        return (
            pl.concat(
                frames,
                how="vertical",
            )
            .sort(
                [
                    "timestamp_ms",
                    "source",
                ]
            )
        )

    # ==================================================================
    # Event conversion
    # ==================================================================

    def to_events(
        self,
        df: pl.DataFrame,
    ) -> list[NormalizedEvent]:
        """
        Convert a normalized dataframe into Python NormalizedEvent objects.

        This method is intended for application-layer processing.

        For large-scale analytical operations, keep using the Polars
        dataframe representation instead of converting everything into
        Python objects.
        """

        self._validate_normalized_schema(df)

        events: list[NormalizedEvent] = []

        for row in df.iter_rows(named=True):
            events.append(
                NormalizedEvent(
                    event_id=row["event_id"],
                    case_id=row["case_id"],
                    timestamp_ms=int(
                        row["timestamp_ms"]
                    ),

                    dataset=self._nullable_string(
                        row["dataset"]
                    ),

                    source=str(row["source"]),

                    service_name=self._nullable_string(
                        row["service_name"]
                    ),

                    signal_type=self._nullable_string(
                        row["signal_type"]
                    ),
                    metric_name=self._nullable_string(
                        row["metric_name"]
                    ),
                    value=self._nullable_float(
                        row["value"]
                    ),

                    message=self._nullable_string(
                        row["message"]
                    ),

                    trace_id=self._nullable_string(
                        row["trace_id"]
                    ),
                    span_id=self._nullable_string(
                        row["span_id"]
                    ),
                    parent_span_id=self._nullable_string(
                        row["parent_span_id"]
                    ),
                    method_name=self._nullable_string(
                        row["method_name"]
                    ),
                    operation_name=self._nullable_string(
                        row["operation_name"]
                    ),

                    duration_ms=self._nullable_float(
                        row["duration_ms"]
                    ),
                    status_code=self._nullable_float(
                        row["status_code"]
                    ),
                )
            )

        return events

    # ==================================================================
    # Metric parsing
    # ==================================================================

    def parse_metric_name(
        self,
        metric_name: str,
    ) -> tuple[str, str]:
        """
        Parse a metric name into:

            (service_name, signal_type)

        Examples
        --------
        >>> parse_metric_name("checkoutservice_cpu")
        ("checkoutservice", "cpu")

        >>> parse_metric_name("frontend_latency-90")
        ("frontend", "latency-90")

        >>> parse_metric_name("carts-db_diskio")
        ("carts-db", "diskio")
        """

        if not metric_name:
            raise ValueError(
                "metric_name cannot be empty."
            )

        metric_name = str(metric_name).strip()

        for suffix in self.METRIC_SUFFIXES:
            marker = f"_{suffix}"

            if metric_name.endswith(marker):
                service_name = metric_name[
                    : -len(marker)
                ]

                if not service_name:
                    break

                return service_name, suffix

        # --------------------------------------------------------------
        # Fallback
        #
        # This is deliberately conservative.
        # We split only once from the right.
        # --------------------------------------------------------------

        if "_" in metric_name:
            service_name, signal_type = (
                metric_name.rsplit("_", 1)
            )

            return service_name, signal_type

        raise ValueError(
            f"Unable to parse metric name: "
            f"'{metric_name}'"
        )

    # ==================================================================
    # Polars expressions
    # ==================================================================

    def _extract_service_expression(
        self,
        column: str,
    ) -> pl.Expr:
        """
        Extract service name from a metric name.

        Examples
        --------
        checkoutservice_cpu -> checkoutservice
        checkoutservice_mem -> checkoutservice
        carts-db_cpu -> carts-db
        ts-route-service_cpu -> ts-route-service
        ts-auth-service_latency-90 -> ts-auth-service
        """

        markers = (
            "_cpu",
            "_mem",
            "_diskio",
            "_socket",
            "_workload",
            "_error",
            "_latency-50",
            "_latency-90",
        )

        expression = None

        for marker in markers:
            condition = pl.col(column).str.ends_with(marker)
            # Use str.replace with regex anchored at end of string
            value = pl.col(column).str.replace(f"{marker}$", "")

            if expression is None:
                expression = pl.when(condition).then(value)
            else:
                expression = expression.when(condition).then(value)

        fallback = pl.col(column).str.replace(r"_[^_]+$", "")

        if expression is None:
            return fallback

        return expression.otherwise(fallback)

    def _extract_signal_type_expression(
        self,
        column: str,
    ) -> pl.Expr:
        """
        Build a Polars expression extracting the signal type.
        """

        expression: Optional[pl.Expr] = None

        for suffix in self.METRIC_SUFFIXES:
            marker = f"_{suffix}"
            condition = pl.col(column).str.ends_with(marker)

            if expression is None:
                expression = pl.when(condition).then(
                    pl.lit(suffix)
                )
            else:
                expression = expression.when(condition).then(
                    pl.lit(suffix)
                )

        # Fallback: extract everything after the last underscore
        fallback = (
            pl.col(column)
            .str.extract(r"([^_]+)$", 1)
        )

        if expression is None:
            return fallback

        return expression.otherwise(fallback)

    # ==================================================================
    # Validation
    # ==================================================================

    @staticmethod
    def _validate_columns(
        df: pl.DataFrame,
        required_columns: set[str],
        source_name: str,
    ) -> None:
        """
        Validate required dataframe columns.
        """

        missing = (
            required_columns
            - set(df.columns)
        )

        if missing:
            raise NormalizationSchemaError(
                f"Invalid {source_name} dataframe. "
                f"Missing columns: "
                f"{sorted(missing)}"
            )

    @staticmethod
    def _validate_normalized_schema(
        df: pl.DataFrame,
    ) -> None:
        """
        Validate the canonical normalized schema.
        """

        required = {
            "event_id",
            "case_id",
            "timestamp_ms",
            "dataset",
            "source",
            "service_name",
            "signal_type",
            "metric_name",
            "value",
            "message",
            "trace_id",
            "span_id",
            "parent_span_id",
            "method_name",
            "operation_name",
            "duration_ms",
            "status_code",
        }

        missing = required - set(df.columns)

        if missing:
            raise NormalizationSchemaError(
                "Invalid normalized dataframe. "
                f"Missing columns: {sorted(missing)}"
            )

    # ==================================================================
    # Finalization
    # ==================================================================

    @staticmethod
    def _finalize(
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        """
        Apply final schema guarantees.
        """

        return df.with_columns(
            [
                pl.col("timestamp_ms")
                .cast(pl.Int64),

                pl.col("value")
                .cast(pl.Float64, strict=False),

                pl.col("duration_ms")
                .cast(pl.Float64, strict=False),

                pl.col("status_code")
                .cast(pl.Float64, strict=False),
            ]
        )

    # ==================================================================
    # Empty schema
    # ==================================================================

    @staticmethod
    def empty_dataframe() -> pl.DataFrame:
        """
        Return an empty dataframe respecting the canonical schema.
        """

        return pl.DataFrame(
            {
                "event_id": pl.Series(
                    "event_id",
                    [],
                    dtype=pl.Utf8,
                ),
                "case_id": pl.Series(
                    "case_id",
                    [],
                    dtype=pl.Utf8,
                ),
                "timestamp_ms": pl.Series(
                    "timestamp_ms",
                    [],
                    dtype=pl.Int64,
                ),
                "dataset": pl.Series(
                    "dataset",
                    [],
                    dtype=pl.Utf8,
                ),
                "source": pl.Series(
                    "source",
                    [],
                    dtype=pl.Utf8,
                ),
                "service_name": pl.Series(
                    "service_name",
                    [],
                    dtype=pl.Utf8,
                ),
                "signal_type": pl.Series(
                    "signal_type",
                    [],
                    dtype=pl.Utf8,
                ),
                "metric_name": pl.Series(
                    "metric_name",
                    [],
                    dtype=pl.Utf8,
                ),
                "value": pl.Series(
                    "value",
                    [],
                    dtype=pl.Float64,
                ),
                "message": pl.Series(
                    "message",
                    [],
                    dtype=pl.Utf8,
                ),
                "trace_id": pl.Series(
                    "trace_id",
                    [],
                    dtype=pl.Utf8,
                ),
                "span_id": pl.Series(
                    "span_id",
                    [],
                    dtype=pl.Utf8,
                ),
                "parent_span_id": pl.Series(
                    "parent_span_id",
                    [],
                    dtype=pl.Utf8,
                ),
                "method_name": pl.Series(
                    "method_name",
                    [],
                    dtype=pl.Utf8,
                ),
                "operation_name": pl.Series(
                    "operation_name",
                    [],
                    dtype=pl.Utf8,
                ),
                "duration_ms": pl.Series(
                    "duration_ms",
                    [],
                    dtype=pl.Float64,
                ),
                "status_code": pl.Series(
                    "status_code",
                    [],
                    dtype=pl.Float64,
                ),
            }
        )

    # ==================================================================
    # Utility conversion functions
    # ==================================================================

    @staticmethod
    def _nullable_string(
        value: object,
    ) -> Optional[str]:
        if value is None:
            return None

        return str(value)

    @staticmethod
    def _nullable_float(
        value: object,
    ) -> Optional[float]:
        if value is None:
            return None

        try:
            return float(value)
        except (TypeError, ValueError):
            return None


# ============================================================================
# Convenience function
# ============================================================================


def normalize_case(
    loader,
    case_id: str,
    *,
    include_metrics: bool = True,
    include_logs: bool = True,
    include_traces: bool = True,
) -> pl.DataFrame:
    """
    Convenience function combining DatasetLoader and SchemaNormalizer.

    Example
    -------
    >>> from pathlib import Path
    >>> from src.ingestion.dataset_loader import create_default_loader

    >>> loader = create_default_loader(Path("."))
    >>> events = normalize_case(
    ...     loader,
    ...     "re2ob_checkoutservice_cpu_1"
    ... )

    """

    normalizer = SchemaNormalizer()

    data = loader.load_case(
        case_id,
        include_metrics=include_metrics,
        include_logs=include_logs,
        include_traces=include_traces,
        metrics_long_format=True,
    )

    metrics = data.get("metrics")
    logs = data.get("logs")
    traces = data.get("traces")

    return normalizer.normalize_all(
        metrics=(
            metrics
            if isinstance(metrics, pl.DataFrame)
            else None
        ),
        logs=(
            logs
            if isinstance(logs, pl.DataFrame)
            else None
        ),
        traces=(
            traces
            if isinstance(traces, pl.DataFrame)
            else None
        ),
    )
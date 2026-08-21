"""
ECDT - Phase 2
Dataset loader for RCAEval-derived telemetry data.

Responsibilities
----------------
This module provides a lightweight access layer to the processed
RCAEval data used by ECDT.

It loads:
    - metrics
    - logs
    - traces
    - ground-truth incident metadata

The loader does NOT:
    - normalize service names
    - classify signal types
    - detect anomalies
    - infer root causes

Those responsibilities belong to later stages of the ingestion pipeline.

Timestamp convention
--------------------
All timestamps exposed by this module are normalized to:

    Unix epoch milliseconds

Source formats:
    metrics.time       -> Unix epoch seconds
    logs.timestamp     -> Unix epoch seconds
    traces.startTimeMillis -> Unix epoch milliseconds
    ground_truth       -> Unix epoch seconds

Therefore:
    seconds * 1000 -> milliseconds
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional

import polars as pl


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class DatasetLoaderError(RuntimeError):
    """Base exception raised by the dataset loader."""


class DatasetSchemaError(DatasetLoaderError):
    """Raised when a dataset does not contain the expected columns."""


class CaseNotFoundError(DatasetLoaderError):
    """Raised when a requested case does not exist."""


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DatasetPaths:
    """
    Paths to the processed ECDT datasets.

    Parameters
    ----------
    metrics_path:
        Path to target_metrics.csv.

    logs_path:
        Path to target_logs.csv.

    traces_path:
        Path to target_traces.csv.

    ground_truth_path:
        Path to target_incidents.csv.
    """

    metrics_path: Path
    logs_path: Path
    traces_path: Path
    ground_truth_path: Path

    @classmethod
    def from_project_root(cls, root: Path) -> "DatasetPaths":
        """
        Build dataset paths from the ECDT project root.
        """

        root = Path(root)

        return cls(
            metrics_path=root
            / "data"
            / "processed"
            / "metrics"
            / "target_metrics.csv",

            logs_path=root
            / "data"
            / "processed"
            / "logs"
            / "target_logs.csv",

            traces_path=root
            / "data"
            / "processed"
            / "traces"
            / "target_traces.csv",

            ground_truth_path=root
            / "data"
            / "ground_truth"
            / "target_incidents.csv",
        )


# ---------------------------------------------------------------------------
# Case information
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CaseInfo:
    """
    Ground-truth metadata for a single RCAEval/ECDT case.

    All timestamps exposed here are Unix epoch milliseconds.
    """

    case_id: str
    dataset: str
    fault: str
    root_cause_service: str

    time_start_ms: int
    inject_time_ms: int
    time_end_ms: int

    incident_type: Optional[str] = None


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------


class DatasetLoader:
    """
    Loader for the processed RCAEval-derived ECDT datasets.

    The loader is intentionally limited to data access.

    Example
    -------
    >>> paths = DatasetPaths.from_project_root(Path("."))
    >>> loader = DatasetLoader(paths)
    >>>
    >>> metrics = loader.load_metrics(case_id="re2ob_checkoutservice_cpu_1")
    >>> logs = loader.load_logs(case_id="re2ob_checkoutservice_cpu_1")
    >>> traces = loader.load_traces(case_id="re2ob_checkoutservice_cpu_1")
    """

    # ------------------------------------------------------------------
    # Expected schemas
    # ------------------------------------------------------------------

    METRICS_METADATA_COLUMNS = {
        "case",
        "dataset",
        "fault",
        "root_cause_service",
        "time",
    }

    LOG_COLUMNS = {
        "case",
        "dataset",
        "fault",
        "root_cause_service",
        "timestamp",
        "container_name",
        "message",
    }

    TRACE_COLUMNS = {
        "case",
        "dataset",
        "fault",
        "root_cause_service",
        "time",
        "traceID",
        "spanID",
        "serviceName",
        "methodName",
        "operationName",
        "parentSpanID",
        "startTimeMillis",
        "startTime",
        "duration",
        "statusCode",
    }

    GROUND_TRUTH_BASE_COLUMNS = {
        "case",
        "dataset",
        "fault",
        "root_cause_service",
    }

    # ------------------------------------------------------------------
    # Constructor
    # ------------------------------------------------------------------

    def __init__(
        self,
        paths: DatasetPaths,
        *,
        validate_paths: bool = True,
    ) -> None:
        self.paths = paths

        if validate_paths:
            self._validate_paths()

    # ------------------------------------------------------------------
    # Path validation
    # ------------------------------------------------------------------

    def _validate_paths(self) -> None:
        """
        Validate that the expected dataset files exist.
        """

        paths = {
            "metrics": self.paths.metrics_path,
            "logs": self.paths.logs_path,
            "traces": self.paths.traces_path,
            "ground_truth": self.paths.ground_truth_path,
        }

        missing = {
            name: path
            for name, path in paths.items()
            if not path.exists()
        }

        if missing:
            details = "\n".join(
                f"  - {name}: {path}"
                for name, path in missing.items()
            )

            raise FileNotFoundError(
                "The following ECDT dataset files were not found:\n"
                f"{details}"
            )

    # ------------------------------------------------------------------
    # Generic schema validation
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_columns(
        available_columns: list[str],
        required_columns: set[str],
        source_name: str,
    ) -> None:
        """
        Validate that all required columns are present.
        """

        missing = required_columns - set(available_columns)

        if missing:
            raise DatasetSchemaError(
                f"Invalid {source_name} schema. "
                f"Missing columns: {sorted(missing)}"
            )

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------

    def load_metrics(
        self,
        case_id: Optional[str] = None,
        *,
        time_start_ms: Optional[int] = None,
        time_end_ms: Optional[int] = None,
        long_format: bool = False,
    ) -> pl.DataFrame:
        """
        Load metric observations.

        Parameters
        ----------
        case_id:
            Optional RCAEval case identifier.

        time_start_ms:
            Optional lower timestamp bound in Unix milliseconds.

        time_end_ms:
            Optional upper timestamp bound in Unix milliseconds.

        long_format:
            If False, preserve the original wide representation.

            If True, transform:

                checkoutservice_cpu
                checkoutservice_mem
                ...

            into:

                metric_name | value

        Returns
        -------
        pl.DataFrame
        """

        lf = pl.scan_csv(
            self.paths.metrics_path,
            try_parse_dates=False,
            infer_schema_length=1000,
        )

        available_columns = lf.collect_schema().names()

        self._validate_columns(
            available_columns,
            self.METRICS_METADATA_COLUMNS,
            "metrics",
        )

        # --------------------------------------------------------------
        # Case filtering
        # --------------------------------------------------------------

        if case_id is not None:
            lf = lf.filter(
                pl.col("case") == case_id
            )

        # --------------------------------------------------------------
        # Time filtering
        #
        # Source metric timestamps are Unix seconds.
        # User-facing API uses milliseconds.
        # --------------------------------------------------------------

        if time_start_ms is not None:
            start_seconds = time_start_ms // 1000

            lf = lf.filter(
                pl.col("time") >= start_seconds
            )

        if time_end_ms is not None:
            end_seconds = time_end_ms // 1000

            lf = lf.filter(
                pl.col("time") <= end_seconds
            )

        # --------------------------------------------------------------
        # Convert timestamps to canonical milliseconds.
        # --------------------------------------------------------------

        lf = lf.with_columns(
            (
                pl.col("time").cast(pl.Int64) * 1000
            ).alias("timestamp")
        )

        # --------------------------------------------------------------
        # Optional wide -> long conversion.
        # --------------------------------------------------------------

        if long_format:
            metadata_columns = [
                "case",
                "dataset",
                "fault",
                "root_cause_service",
                "time",
                "timestamp",
            ]

            metric_columns = [
                column
                for column in available_columns
                if column not in self.METRICS_METADATA_COLUMNS
            ]

            lf = (
                lf.unpivot(
                    on=metric_columns,
                    index=metadata_columns,
                    variable_name="metric_name",
                    value_name="value",
                )
                .with_columns(
                    pl.col("value")
                    .cast(pl.Float64, strict=False)
                )
            )

        return lf.collect()

    # ------------------------------------------------------------------
    # Metric lazy loader
    # ------------------------------------------------------------------

    def scan_metrics(
        self,
        case_id: Optional[str] = None,
        *,
        time_start_ms: Optional[int] = None,
        time_end_ms: Optional[int] = None,
        long_format: bool = False,
    ) -> pl.LazyFrame:
        """
        Return a lazy metrics query.

        This method is useful when downstream processing should remain
        lazy and avoid materializing the entire dataset.
        """

        lf = pl.scan_csv(
            self.paths.metrics_path,
            try_parse_dates=False,
            infer_schema_length=1000,
        )

        available_columns = lf.collect_schema().names()

        self._validate_columns(
            available_columns,
            self.METRICS_METADATA_COLUMNS,
            "metrics",
        )

        if case_id is not None:
            lf = lf.filter(
                pl.col("case") == case_id
            )

        if time_start_ms is not None:
            start_seconds = time_start_ms // 1000

            lf = lf.filter(
                pl.col("time") >= start_seconds
            )

        if time_end_ms is not None:
            end_seconds = time_end_ms // 1000

            lf = lf.filter(
                pl.col("time") <= end_seconds
            )

        lf = lf.with_columns(
            (
                pl.col("time").cast(pl.Int64) * 1000
            ).alias("timestamp")
        )

        if long_format:
            metadata_columns = [
                "case",
                "dataset",
                "fault",
                "root_cause_service",
                "time",
                "timestamp",
            ]

            metric_columns = [
                column
                for column in available_columns
                if column not in self.METRICS_METADATA_COLUMNS
            ]

            lf = lf.unpivot(
                on=metric_columns,
                index=metadata_columns,
                variable_name="metric_name",
                value_name="value",
            )

        return lf

    # ------------------------------------------------------------------
    # Logs
    # ------------------------------------------------------------------

    def load_logs(
        self,
        case_id: Optional[str] = None,
        *,
        time_start_ms: Optional[int] = None,
        time_end_ms: Optional[int] = None,
    ) -> pl.DataFrame:
        """
        Load log events.

        The source timestamp is in Unix seconds and is converted to
        canonical Unix milliseconds.
        """

        lf = pl.scan_csv(
            self.paths.logs_path,
            try_parse_dates=False,
            infer_schema_length=1000,
        )

        available_columns = lf.collect_schema().names()

        self._validate_columns(
            available_columns,
            self.LOG_COLUMNS,
            "logs",
        )

        if case_id is not None:
            lf = lf.filter(
                pl.col("case") == case_id
            )

        if time_start_ms is not None:
            start_seconds = time_start_ms // 1000

            lf = lf.filter(
                pl.col("timestamp") >= start_seconds
            )

        if time_end_ms is not None:
            end_seconds = time_end_ms // 1000

            lf = lf.filter(
                pl.col("timestamp") <= end_seconds
            )

        lf = lf.with_columns(
            (
                pl.col("timestamp").cast(pl.Int64) * 1000
            ).alias("timestamp_ms")
        )

        return lf.collect()

    # ------------------------------------------------------------------
    # Traces
    # ------------------------------------------------------------------

    def load_traces(
        self,
        case_id: Optional[str] = None,
        *,
        time_start_ms: Optional[int] = None,
        time_end_ms: Optional[int] = None,
    ) -> pl.DataFrame:
        """
        Load trace/span events.

        The trace source already provides startTimeMillis in Unix
        milliseconds, so no conversion is necessary for that field.
        """

        lf = pl.scan_csv(
            self.paths.traces_path,
            try_parse_dates=False,
            infer_schema_length=1000,
        )

        available_columns = lf.collect_schema().names()

        self._validate_columns(
            available_columns,
            self.TRACE_COLUMNS,
            "traces",
        )

        if case_id is not None:
            lf = lf.filter(
                pl.col("case") == case_id
            )

        if time_start_ms is not None:
            lf = lf.filter(
                pl.col("startTimeMillis") >= time_start_ms
            )

        if time_end_ms is not None:
            lf = lf.filter(
                pl.col("startTimeMillis") <= time_end_ms
            )

        # Explicit canonical timestamp column.
        lf = lf.with_columns(
            pl.col("startTimeMillis")
            .cast(pl.Int64)
            .alias("timestamp_ms")
        )

        return lf.collect()

    # ------------------------------------------------------------------
    # Chunk iteration
    # ------------------------------------------------------------------

    def iter_metrics(
        self,
        case_id: Optional[str] = None,
        *,
        time_start_ms: Optional[int] = None,
        time_end_ms: Optional[int] = None,
        long_format: bool = False,
        chunk_size: int = 10_000,
    ) -> Iterator[pl.DataFrame]:
        """
        Iterate over metric rows in chunks.

        Note
        ----
        This method materializes the filtered query before slicing.
        It therefore limits the working set to the selected case/window,
        but is not a true streaming CSV reader.
        """

        if chunk_size <= 0:
            raise ValueError("chunk_size must be greater than zero.")

        df = self.load_metrics(
            case_id=case_id,
            time_start_ms=time_start_ms,
            time_end_ms=time_end_ms,
            long_format=long_format,
        )

        for chunk in df.iter_slices(
            n_rows=chunk_size
        ):
            yield chunk

    # ------------------------------------------------------------------
    # Case inventory
    # ------------------------------------------------------------------

    def list_cases(self) -> list[str]:
        """
        Return all unique case identifiers.

        The metrics file is used as the canonical inventory source.
        """

        lf = pl.scan_csv(
            self.paths.metrics_path,
            try_parse_dates=False,
            infer_schema_length=1000,
        )

        self._validate_columns(
            lf.collect_schema().names(),
            {"case"},
            "metrics",
        )

        cases = (
            lf.select("case")
            .unique()
            .collect()
            .get_column("case")
            .drop_nulls()
            .to_list()
        )

        return sorted(str(case) for case in cases)

    # ------------------------------------------------------------------
    # Ground truth
    # ------------------------------------------------------------------

    def load_ground_truth(
        self,
        case_id: Optional[str] = None,
    ) -> pl.DataFrame:
        """
        Load the ECDT ground-truth incident table.

        Ground-truth fields remain separate from telemetry observations.
        """

        lf = pl.scan_csv(
            self.paths.ground_truth_path,
            try_parse_dates=False,
            infer_schema_length=1000,
        )

        available_columns = lf.collect_schema().names()

        self._validate_columns(
            available_columns,
            self.GROUND_TRUTH_BASE_COLUMNS,
            "ground_truth",
        )

        if case_id is not None:
            lf = lf.filter(
                pl.col("case") == case_id
            )

        return lf.collect()

    # ------------------------------------------------------------------
    # Case information
    # ------------------------------------------------------------------

    def get_case_info(
        self,
        case_id: str,
    ) -> CaseInfo:
        """
        Retrieve ground-truth information for one case.

        This method expects the target_incidents.csv file to contain
        the incident timing information.

        The method supports the following possible timestamp columns:

            time_start
            inject_time
            time_end

        If these columns are unavailable, a DatasetSchemaError is raised.
        """

        df = self.load_ground_truth(case_id=case_id)

        if df.is_empty():
            raise CaseNotFoundError(
                f"Case '{case_id}' was not found in ground truth."
            )

        required_timing_columns = {
            "time_start",
            "inject_time",
            "time_end",
        }

        self._validate_columns(
            df.columns,
            required_timing_columns,
            "ground_truth timing",
        )

        row = df.row(0, named=True)

        incident_type = row.get("incident_type")

        return CaseInfo(
            case_id=str(row["case"]),
            dataset=str(row["dataset"]),
            fault=str(row["fault"]),
            root_cause_service=str(
                row["root_cause_service"]
            ),
            time_start_ms=self._seconds_to_ms(
                row["time_start"]
            ),
            inject_time_ms=self._seconds_to_ms(
                row["inject_time"]
            ),
            time_end_ms=self._seconds_to_ms(
                row["time_end"]
            ),
            incident_type=(
                str(incident_type)
                if incident_type is not None
                else None
            ),
        )

    # ------------------------------------------------------------------
    # Time window
    # ------------------------------------------------------------------

    def get_case_time_window(
        self,
        case_id: str,
    ) -> tuple[int, int, int]:
        """
        Return:

            (time_start_ms, inject_time_ms, time_end_ms)

        for a case.
        """

        info = self.get_case_info(case_id)

        return (
            info.time_start_ms,
            info.inject_time_ms,
            info.time_end_ms,
        )

    # ------------------------------------------------------------------
    # Metric column discovery
    # ------------------------------------------------------------------

    def get_metric_columns(self) -> list[str]:
        """
        Return all telemetry metric columns.

        Metadata columns are excluded.
        """

        lf = pl.scan_csv(
            self.paths.metrics_path,
            try_parse_dates=False,
            infer_schema_length=1000,
        )

        columns = lf.collect_schema().names()

        return [
            column
            for column in columns
            if column not in self.METRICS_METADATA_COLUMNS
        ]

    # ------------------------------------------------------------------
    # Case-level telemetry
    # ------------------------------------------------------------------

    def load_case(
        self,
        case_id: str,
        *,
        include_metrics: bool = True,
        include_logs: bool = True,
        include_traces: bool = True,
        metrics_long_format: bool = False,
    ) -> dict[str, pl.DataFrame | CaseInfo]:
        """
        Load all telemetry associated with one case.

        Returns
        -------
        dict
            Dictionary containing:

                case_info
                metrics
                logs
                traces
        """

        case_info = self.get_case_info(case_id)

        result: dict[str, pl.DataFrame | CaseInfo] = {
            "case_info": case_info,
        }

        if include_metrics:
            result["metrics"] = self.load_metrics(
                case_id=case_id,
                time_start_ms=case_info.time_start_ms,
                time_end_ms=case_info.time_end_ms,
                long_format=metrics_long_format,
            )

        if include_logs:
            result["logs"] = self.load_logs(
                case_id=case_id,
                time_start_ms=case_info.time_start_ms,
                time_end_ms=case_info.time_end_ms,
            )

        if include_traces:
            result["traces"] = self.load_traces(
                case_id=case_id,
                time_start_ms=case_info.time_start_ms,
                time_end_ms=case_info.time_end_ms,
            )

        return result

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _seconds_to_ms(value: object) -> int:
        """
        Convert a Unix timestamp expressed in seconds to milliseconds.
        """

        if value is None:
            raise ValueError(
                "Cannot convert a null timestamp to milliseconds."
            )

        return int(float(value) * 1000)


# ---------------------------------------------------------------------------
# Convenience factory
# ---------------------------------------------------------------------------


def create_default_loader(
    project_root: Optional[Path] = None,
) -> DatasetLoader:
    """
    Create a DatasetLoader using the ECDT project structure.

    If project_root is omitted, the current working directory is used.
    """

    root = (
        Path(project_root)
        if project_root is not None
        else Path.cwd()
    )

    paths = DatasetPaths.from_project_root(root)

    return DatasetLoader(paths)
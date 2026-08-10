# SPDX-FileCopyrightText: 2024-present Mark Janse <mark.janse@health-ri.nl>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
"""
Logging configuration for the harvester.

Configured entirely from environment variables, so a scheduled run needs no code changes:

LOG_LEVEL           INFO, WARNING or ERROR (default INFO).
LOG_FILE            Base path for file logging; file logging is off when unset. Every run
                    writes its own file, with the run's start time inserted before the
                    suffix ("/var/log/harvest.log" -> "/var/log/harvest_20240131-021500.log"),
                    so no single file grows without bound.
LOG_FILE_RETENTION  How many run logs to keep (default 10). Once the current run's file has
                    been created, older ones are deleted.

Logs always go to stdout as well, so cron/container output stays useful even when file
logging is off or could not be set up.

Give LOG_FILE a directory of its own: pruning deletes every sibling matching the run-log
naming scheme, not just files this process created.
"""
import glob
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

DEFAULT_LOG_LEVEL = "INFO"
VALID_LOG_LEVELS = {"INFO", "WARNING", "ERROR"}
LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
DEFAULT_LOG_FILE_RETENTION = 10
RUN_TIMESTAMP_FORMAT = "%Y%m%d-%H%M%S"
# Deliberately narrow, so an unrelated neighbouring file is never a pruning candidate.
_RUN_TIMESTAMP_GLOB = "_" + "[0-9]" * 8 + "-" + "[0-9]" * 6


def run_log_path(log_file: str, timestamp: datetime | None = None) -> Path:
    """Return the per-run log path derived from the LOG_FILE base path."""
    base = Path(log_file)
    stamp = (timestamp or datetime.now()).strftime(RUN_TIMESTAMP_FORMAT)
    return base.with_name(f"{base.stem}_{stamp}{base.suffix}")


def prune_old_run_logs(log_file: str, retention: int) -> list[str]:
    """Delete all but the `retention` most recent run logs for the given LOG_FILE base path.

    Returns descriptions of anything that went wrong. Pruning is best-effort and never
    raises: failing to tidy up old logs is no reason to abort a harvest.
    """
    base = Path(log_file)
    pattern = f"{glob.escape(base.stem)}{_RUN_TIMESTAMP_GLOB}{glob.escape(base.suffix)}"
    try:
        # Timestamps are fixed-width and zero-padded, so sorting by name sorts by age.
        run_logs = sorted(path for path in base.parent.glob(pattern) if path.is_file())
    except OSError as exc:
        return [f"Could not list old run logs in '{base.parent}': {exc}"]

    problems = []
    for stale_log in run_logs[:-retention]:
        try:
            stale_log.unlink()
        except OSError as exc:
            problems.append(f"Could not delete old run log '{stale_log}': {exc}")
    return problems


def _resolve_log_level(problems: list[str]) -> str:
    requested_level = os.environ.get("LOG_LEVEL", DEFAULT_LOG_LEVEL).strip().upper()
    if requested_level in VALID_LOG_LEVELS:
        return requested_level

    problems.append(
        f"Invalid LOG_LEVEL '{requested_level}'; expected one of {sorted(VALID_LOG_LEVELS)}. "
        f"Defaulting to {DEFAULT_LOG_LEVEL}."
    )
    return DEFAULT_LOG_LEVEL


def _resolve_retention(problems: list[str]) -> int:
    raw_retention = os.environ.get("LOG_FILE_RETENTION", "").strip()
    if not raw_retention:
        return DEFAULT_LOG_FILE_RETENTION

    try:
        retention = int(raw_retention)
    except ValueError:
        retention = 0

    if retention < 1:
        problems.append(
            f"Invalid LOG_FILE_RETENTION '{raw_retention}'; expected a positive integer. "
            f"Defaulting to {DEFAULT_LOG_FILE_RETENTION}."
        )
        return DEFAULT_LOG_FILE_RETENTION
    return retention


def configure_logging() -> None:
    """Configure the root logger from the LOG_LEVEL, LOG_FILE and LOG_FILE_RETENTION vars."""
    # Collected, not logged: a problem with the logging config can only be reported once
    # logging itself works.
    problems: list[str] = []
    level_name = _resolve_log_level(problems)
    retention = _resolve_retention(problems)

    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    log_file = os.environ.get("LOG_FILE", "").strip()
    current_run_log = None
    if log_file:
        current_run_log = run_log_path(log_file)
        try:
            current_run_log.parent.mkdir(parents=True, exist_ok=True)
            # Explicit encoding: RDF titles are routinely non-ASCII, and the platform
            # default would silently drop those records under a C locale.
            handlers.append(logging.FileHandler(current_run_log, encoding="utf-8"))
        except OSError as exc:
            # An unusable log file must not take down a scheduled harvest.
            problems.append(
                f"Could not open log file '{current_run_log}': {exc}. Logging to stdout only."
            )
            current_run_log = None

    # force=True: reconfigure even if a handler was already attached to the root logger
    # (e.g. a previous call, or a library that configured logging on import).
    logging.basicConfig(level=level_name, format=LOG_FORMAT, handlers=handlers, force=True)

    log = logging.getLogger(__name__)
    if current_run_log is not None:
        log.info(
            "Writing this run's log to %s (keeping the %d most recent run logs)",
            current_run_log, retention,
        )
        problems.extend(prune_old_run_logs(log_file, retention))

    for problem in problems:
        log.warning("%s", problem)

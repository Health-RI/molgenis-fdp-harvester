import logging
import re
from datetime import datetime, timedelta

import pytest

from molgenis_fdp_harvester.logging_config import (
    DEFAULT_LOG_FILE_RETENTION,
    configure_logging,
    prune_old_run_logs,
    run_log_path,
)


@pytest.fixture(autouse=True)
def restore_root_logger():
    root = logging.getLogger()
    original_handlers = root.handlers[:]
    original_level = root.level
    yield
    for handler in root.handlers[:]:
        root.removeHandler(handler)
        # close handlers configure_logging opened, so the files can be removed again
        if handler not in original_handlers:
            handler.close()
    for handler in original_handlers:
        root.addHandler(handler)
    root.setLevel(original_level)


@pytest.fixture(autouse=True)
def clean_logging_env(monkeypatch):
    for name in ("LOG_LEVEL", "LOG_FILE", "LOG_FILE_RETENTION"):
        monkeypatch.delenv(name, raising=False)


_RUN_LOG_SUFFIX = re.compile(r"_\d{8}-\d{6}$")


def _run_logs(log_file):
    """Names of the timestamped run logs belonging to the given base path."""
    return sorted(
        path.name
        for path in log_file.parent.glob(f"{log_file.stem}_*{log_file.suffix}")
        if _RUN_LOG_SUFFIX.search(path.stem)
    )


def test_configure_logging_defaults_to_info():
    configure_logging()

    assert logging.getLogger().level == logging.INFO


@pytest.mark.parametrize("level_name,expected", [
    ("WARNING", logging.WARNING),
    ("ERROR", logging.ERROR),
    ("info", logging.INFO),
])
def test_configure_logging_respects_log_level_env_var(monkeypatch, level_name, expected):
    monkeypatch.setenv("LOG_LEVEL", level_name)

    configure_logging()

    assert logging.getLogger().level == expected


def test_configure_logging_invalid_level_falls_back_to_info(monkeypatch, capsys):
    monkeypatch.setenv("LOG_LEVEL", "NOT_A_LEVEL")

    # asserted on stdout rather than caplog: configure_logging reconfigures the root
    # logger with force=True, which detaches pytest's capture handler
    configure_logging()

    assert logging.getLogger().level == logging.INFO
    assert "Invalid LOG_LEVEL" in capsys.readouterr().out


def test_configure_logging_without_log_file_only_logs_to_stdout():
    configure_logging()

    handler_types = [type(h) for h in logging.getLogger().handlers]
    assert logging.FileHandler not in handler_types


def test_configure_logging_writes_to_a_per_run_file(monkeypatch, tmp_path):
    log_file = tmp_path / "harvester.log"
    monkeypatch.setenv("LOG_FILE", str(log_file))

    configure_logging()
    logging.getLogger("molgenis_fdp_harvester.test_logging_config").info("hello from test")
    for handler in logging.getLogger().handlers:
        handler.flush()

    # the base path itself is never written to; each run gets its own timestamped file
    assert not log_file.exists()
    written = list(tmp_path.glob("harvester_*.log"))
    assert len(written) == 1
    assert "hello from test" in written[0].read_text(encoding="utf-8")


def test_run_log_file_is_utf8_regardless_of_platform_default(monkeypatch, tmp_path):
    """RDF titles are routinely non-ASCII; a locale-encoded handler would drop them."""
    monkeypatch.setenv("LOG_FILE", str(tmp_path / "harvester.log"))

    configure_logging()
    logging.getLogger("molgenis_fdp_harvester.test_logging_config").info("Santé Publique")
    for handler in logging.getLogger().handlers:
        handler.flush()

    file_handlers = [h for h in logging.getLogger().handlers if isinstance(h, logging.FileHandler)]
    assert [h.encoding for h in file_handlers] == ["utf-8"]
    written = next(iter(tmp_path.glob("harvester_*.log")))
    assert "Santé Publique" in written.read_text(encoding="utf-8")


def test_configure_logging_creates_missing_log_directory(monkeypatch, tmp_path):
    log_file = tmp_path / "nested" / "dir" / "harvester.log"
    monkeypatch.setenv("LOG_FILE", str(log_file))

    configure_logging()

    assert len(list(log_file.parent.glob("harvester_*.log"))) == 1


def test_configure_logging_falls_back_to_stdout_when_log_file_unusable(monkeypatch, tmp_path, capsys):
    # a file where a directory needs to be makes the log path impossible to create
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory")
    monkeypatch.setenv("LOG_FILE", str(blocker / "harvester.log"))

    configure_logging()  # must not raise: a broken LOG_FILE may not abort a scheduled run

    handler_types = [type(h) for h in logging.getLogger().handlers]
    assert logging.FileHandler not in handler_types
    assert "Logging to stdout only" in capsys.readouterr().out


def test_old_run_logs_beyond_retention_are_deleted(monkeypatch, tmp_path):
    log_file = tmp_path / "harvester.log"
    monkeypatch.setenv("LOG_FILE", str(log_file))
    monkeypatch.setenv("LOG_FILE_RETENTION", "3")

    start = datetime(2024, 1, 1, 2, 0, 0)
    for offset in range(5):
        run_log_path(str(log_file), start + timedelta(days=offset)).write_text("old run")

    configure_logging()

    remaining = _run_logs(log_file)
    # 3 kept in total: the current run's file plus the 2 most recent old ones
    assert len(remaining) == 3
    assert "harvester_20240101-020000.log" not in remaining
    assert "harvester_20240102-020000.log" not in remaining
    assert "harvester_20240105-020000.log" in remaining


def test_run_logs_default_to_keeping_ten(monkeypatch, tmp_path):
    log_file = tmp_path / "harvester.log"
    monkeypatch.setenv("LOG_FILE", str(log_file))

    start = datetime(2024, 1, 1, 2, 0, 0)
    for offset in range(15):
        run_log_path(str(log_file), start + timedelta(days=offset)).write_text("old run")

    configure_logging()

    assert len(_run_logs(log_file)) == DEFAULT_LOG_FILE_RETENTION


@pytest.mark.parametrize("raw_retention", ["zero", "0", "-3"])
def test_invalid_retention_falls_back_to_default(monkeypatch, tmp_path, capsys, raw_retention):
    log_file = tmp_path / "harvester.log"
    monkeypatch.setenv("LOG_FILE", str(log_file))
    monkeypatch.setenv("LOG_FILE_RETENTION", raw_retention)

    start = datetime(2024, 1, 1, 2, 0, 0)
    for offset in range(12):
        run_log_path(str(log_file), start + timedelta(days=offset)).write_text("old run")

    configure_logging()

    assert "Invalid LOG_FILE_RETENTION" in capsys.readouterr().out
    assert len(_run_logs(log_file)) == DEFAULT_LOG_FILE_RETENTION


def test_pruning_leaves_unrelated_files_alone(tmp_path):
    log_file = tmp_path / "harvester.log"
    start = datetime(2024, 1, 1, 2, 0, 0)
    for offset in range(3):
        run_log_path(str(log_file), start + timedelta(days=offset)).write_text("old run")

    bystanders = [tmp_path / "harvester.log", tmp_path / "harvester_notes.log", tmp_path / "other_20240101-020000.log"]
    for bystander in bystanders:
        bystander.write_text("keep me")

    assert prune_old_run_logs(str(log_file), 1) == []

    assert len(_run_logs(log_file)) == 1
    for bystander in bystanders:
        assert bystander.exists()


def test_pruning_never_raises_when_directory_is_missing(tmp_path):
    assert prune_old_run_logs(str(tmp_path / "gone" / "harvester.log"), 5) == []

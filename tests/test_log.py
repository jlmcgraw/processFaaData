"""Tests for the tiny stderr progress logger."""

from __future__ import annotations

import pytest

from faa_nasr import _log


@pytest.fixture(autouse=True)
def reset_quiet() -> None:
    """Reset the module-level quiet flag between tests so they don't interfere."""
    _log.set_quiet(False)


def test_set_quiet_toggles_is_quiet():
    assert _log.is_quiet() is False
    _log.set_quiet(True)
    assert _log.is_quiet() is True
    _log.set_quiet(False)
    assert _log.is_quiet() is False


def test_info_writes_to_stderr_when_not_quiet(capsys):
    _log.info("hello")
    captured = capsys.readouterr()
    assert captured.err == "hello\n"
    assert captured.out == ""


def test_info_silent_when_quiet(capsys):
    _log.set_quiet(True)
    _log.info("hello")
    captured = capsys.readouterr()
    assert captured.err == ""


def test_step_prefixes_with_marker(capsys):
    _log.step("doing thing")
    captured = capsys.readouterr()
    assert captured.err == "==> doing thing\n"


def test_step_silent_when_quiet(capsys):
    _log.set_quiet(True)
    _log.step("doing thing")
    captured = capsys.readouterr()
    assert captured.err == ""

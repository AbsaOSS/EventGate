#
# Copyright 2026 ABSA Group Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging

import pytest
from aws_lambda_powertools import Logger

from src.utils.logging_levels import TRACE_LEVEL, configured_log_level, invalid_log_level, resolve_log_level


@pytest.fixture(autouse=True)
def clear_log_level_env(monkeypatch):
    """Start every test from an unset log level configuration."""
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    monkeypatch.delenv("POWERTOOLS_LOG_LEVEL", raising=False)


## configured_log_level()
def test_configured_log_level_defaults_to_info():
    assert "INFO" == configured_log_level()


def test_powertools_log_level_takes_precedence(monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("POWERTOOLS_LOG_LEVEL", "warning")

    assert "WARNING" == configured_log_level()


## resolve_log_level()
@pytest.mark.parametrize(
    "configured,expected",
    [("TRACE", TRACE_LEVEL), ("debug", logging.DEBUG), ("INFO", logging.INFO), ("ERROR", logging.ERROR)],
)
def test_resolve_log_level_known_levels(monkeypatch, configured, expected):
    monkeypatch.setenv("LOG_LEVEL", configured)

    assert expected == resolve_log_level()
    assert invalid_log_level() is None


def test_resolve_log_level_falls_back_to_info(monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "NOT_A_LEVEL")

    assert logging.INFO == resolve_log_level()
    assert "NOT_A_LEVEL" == invalid_log_level()


## Powertools compatibility
def test_powertools_logger_accepts_the_custom_trace_level(monkeypatch, capsys):
    monkeypatch.setenv("LOG_LEVEL", "TRACE")

    powertools_logger = Logger(service="eventgate-trace-check", level=resolve_log_level())
    powertools_logger.trace("Payload.")

    assert TRACE_LEVEL == powertools_logger.log_level
    assert '"level":"TRACE"' in capsys.readouterr().out

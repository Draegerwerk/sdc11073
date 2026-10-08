"""Pytest configuration shared by all tests."""

from __future__ import annotations

import os
import pathlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pytest


def _process_name(config: pytest.Config) -> str:
    """Return 'gw0', 'gw1', ... for xdist workers, 'controller' for the xdist controller and 'main' otherwise."""
    worker_id = os.environ.get('PYTEST_XDIST_WORKER')
    if worker_id is not None:
        return worker_id
    if config.getoption('numprocesses', default=None):
        return 'controller'
    return 'main'


def pytest_configure(config: pytest.Config):
    """Give each process its own log file, otherwise xdist workers overwrite each other's log.

    The directory of the configured log file is kept, only the file name is replaced by the process name.
    """
    log_file = config.getoption('log_file', default=None) or config.getini('log_file')
    if not log_file:
        return
    log_path = pathlib.Path(log_file)
    config.option.log_file = str(log_path.with_name(f'{_process_name(config)}{log_path.suffix or ".log"}'))

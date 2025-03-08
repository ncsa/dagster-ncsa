from __future__ import annotations

import importlib.metadata

import dagster_ncsa as m


def test_version():
    assert importlib.metadata.version("dagster_ncsa") == m.__version__

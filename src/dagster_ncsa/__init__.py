"""
Copyright (c) 2025 Ben Galewsky. All rights reserved.

dagster-ncsa: A great package.A Python library providing useful components for using [Dagster](https://dagster.io/) to create academic research cloud datalakes
"""

from __future__ import annotations

from ._version import version as __version__
from .s3_resource_ncsa import S3ResourceNCSA

__all__ = ["S3ResourceNCSA", "__version__"]

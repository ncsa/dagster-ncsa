# dagster-ncsa

A Python library providing useful components for using
[Dagster](https://dagster.io/) to create academic research cloud data lakes for
the National Center for Supercomputing Applications (NCSA).

## Overview

`dagster-ncsa` extends Dagster's capabilities with specialized tools designed
specifically for academic research workflows and data management at scale. It
provides abstractions and utilities to simplify building, managing, and
monitoring data pipelines in research-oriented cloud data lake environments.

## Components

- **S3ResourceNCSA**: Extends the Dagster S3 resource to add some useful helper
  functions for working with S3 objects in a research data pipeline.
- **AirTableCatalogResource**: A resource for interacting with AirTable tables
  as a catalog for data assets in a research data pipeline.

## Installation

### Basic Installation

```bash
pip install dagster-ncsa
```

### Development Installation

```bash
pip install -e ".[dev]"
```

## Development

### Setup Development Environment

```bash
# Clone the repository
git clone https://github.com/your-organization/dagster-ncsa.git
cd dagster-ncsa

# Install development dependencies
pip install -e ".[dev]
```

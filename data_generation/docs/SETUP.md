# Data Generation Project Setup Guide

## Overview

This data generation project is part of the Summit Sports analytics suite. It's designed to create synthetic data for testing, development, and demonstration purposes.

## Environment Setup

### 1. Conda Environment
The project uses the existing `generalenv` conda environment:
```bash
conda activate generalenv
```

### 2. Install Dependencies
Install the data generation specific dependencies:
```bash
cd data_generation
pip install -r requirements.txt
```

### 3. Configuration
Copy and modify the configuration file as needed:
```bash
cp config/config.yaml config/config.local.yaml
```

Set up environment variables for Snowflake connection:
```bash
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_username"
export SNOWFLAKE_PASSWORD="your_password"
export SNOWFLAKE_ROLE="your_role"
export SNOWFLAKE_WAREHOUSE="your_warehouse"
```

## Quick Start

### Generate Sample Customer Data
```bash
cd data_generation/scripts
python customer_generator.py
```

This will generate customer data according to the configuration settings and save it to the `data/` directory.

## Project Structure

- `config/` - Configuration files (YAML format)
- `data/` - Generated data outputs (ignored by git)
- `scripts/` - Python data generation scripts
- `notebooks/` - Jupyter notebooks for exploration
- `docs/` - Documentation
- `tests/` - Unit tests

## Adding New Generators

1. Create a new Python file in `scripts/`
2. Inherit from `BaseDataGenerator`
3. Implement the `generate_data()` method
4. Add configuration parameters to `config.yaml`
5. Add tests in `tests/`

## Git Workflow

- Main development happens in the `data-generation` branch
- Create feature branches from `data-generation` for specific generators
- Merge back to `data-generation` when complete
- Periodically merge `data-generation` to `main`

## Repository Information

- **GitHub Repository**: https://github.com/sfc-gh-edendulk/summit_sports
- **Current Branch**: data-generation
- **Conda Environment**: generalenv

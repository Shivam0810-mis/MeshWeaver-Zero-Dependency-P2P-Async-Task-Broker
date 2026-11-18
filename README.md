# MeshWeaver
## Zero-Dependency P2P Async Task Broker

MeshWeaver is a Python-based peer-to-peer asynchronous task broker.

## Project Objectives

- Distribute tasks between multiple nodes.
- Support asynchronous task execution.
- Enable communication using TCP.
- Exchange messages using JSON.
- Store peer and task information using SQLite.

## Technology Stack

- Python 3.11+
- asyncio
- TCP
- JSON
- SQLite
- unittest
- logging

## Project Structure

- `meshweaver/core` – Core functionality
- `meshweaver/models` – Shared data models
- `meshweaver/config` – Configuration
- `meshweaver/storage` – Database
- `meshweaver/network` – Peer communication
- `meshweaver/broker` – Task scheduling
- `meshweaver/worker` – Task execution
- `meshweaver/cli` – Command-line interface
- `tests` – Automated tests
- `docs` – Documentation

## Team Responsibilities

- Member 1: Network and P2P
- Member 2: Broker and Scheduling
- Member 3: Worker and Async Execution
- Member 4: Integration, Storage and Quality

## Installation

Python 3.11 or newer is required. No third-party runtime dependencies are planned.

## Run

The application entry point is planned as:

```bash
python -m meshweaver.main
```

## Development Setup

MeshWeaver uses Python's standard library for its core functionality.

### Run Tests

From the project root directory, run:

```bash
python -m unittest discover -s tests -v
```

### Check Python Syntax

```bash
python -m compileall meshweaver tests
```

## Architecture Overview

MeshWeaver is a zero-dependency, peer-to-peer asynchronous task broker built using Python's standard library.

### Main Components

- **Network:** Handles TCP communication between peer nodes.
- **Broker:** Manages task submission, queues, and scheduling.
- **Worker:** Executes tasks asynchronously using registered handlers.
- **Models:** Defines shared messages, data structures, and protocol types.
- **Config:** Loads application settings from JSON configuration files.
- **Storage:** Uses SQLite for database connections and persistence.
- **Core:** Contains shared exceptions and common functionality.

### Configuration

The default configuration is stored in:

`meshweaver/config/default_config.json`

The configuration loader is available in:

`meshweaver/config/loader.py`

To load the default configuration:

```python
from meshweaver.config.loader import load_config

config = load_config()
print(config)
```

## Development Workflow

Each member works on their assigned Git branch. Changes are pushed and submitted through pull requests for review before merging into main.
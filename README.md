# MeshWeaver
## Zero-Dependency P2P Async Task Broker

MeshWeaver is a Python-based peer-to-peer asynchronous task broker.

### Project Objectives
- Distribute tasks between multiple nodes.
- Support asynchronous task execution.
- Enable communication using TCP.
- Exchange messages using JSON.
- Store peer and task information using SQLite.

### Technology Stack
- Python 3.11+
- asyncio
- TCP
- JSON
- SQLite
- unittest
- logging

### Project Structure
- meshweaver/core – Core functionality
- meshweaver/models – Shared data models
- meshweaver/config – Configuration
- meshweaver/storage – Database
- meshweaver/network – Peer communication
- meshweaver/broker – Task scheduling
- meshweaver/worker – Task execution
- meshweaver/cli – Command-line interface
- tests – Automated tests
- docs – Documentation

### Team Responsibilities
- Member 1: Network and P2P
- Member 2: Broker and Scheduling
- Member 3: Worker and Async Execution
- Member 4: Integration, Storage and Quality

### Installation
Python 3.11 or newer is required. No third-party runtime dependencies are planned.

### Run
python -m meshweaver.main

### Testing
python -m unittest discover -s tests -v

### Development Workflow
Each member works on their assigned Git branch. Changes are pushed and submitted through pull requests for review before merging into main.

## Development Setup

MeshWeaver uses Python's standard library for its core functionality.

### Run Tests

From the project root directory, run:

```bash
python -m unittest discover -s tests -v
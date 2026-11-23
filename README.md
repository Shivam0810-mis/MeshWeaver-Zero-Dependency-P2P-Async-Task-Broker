# MeshWeaver
## Zero-Dependency P2P Async Task Broker

MeshWeaver is a Python-based asynchronous task broker project designed to support task distribution, peer management, scheduling, and worker execution using Python's standard library.

The project focuses on building a lightweight, modular task-brokering system with SQLite persistence and clearly defined component interfaces.

## Project Objectives

- Register and manage asynchronous tasks.
- Maintain a task queue and task lifecycle.
- Select eligible peers using scheduling logic.
- Dispatch tasks through a transport abstraction.
- Execute registered task handlers asynchronously.
- Persist peer and task information using SQLite.
- Exchange structured messages using shared protocol models.
- Provide structured logging and automated testing.

## Technology Stack

- Python 3.11+
- asyncio
- SQLite
- JSON
- unittest
- pytest (development and testing)
- Python logging
- Git and GitHub

## Project Structure

```text
MeshWeaver/
│
├── meshweaver/
│   ├── broker/
│   │   ├── broker.py
│   │   ├── dispatcher.py
│   │   ├── interfaces.py
│   │   ├── models.py
│   │   ├── peers.py
│   │   ├── queue.py
│   │   └── scheduler.py
│   │
│   ├── config/
│   │   └── loader.py
│   │
│   ├── core/
│   │   ├── errors.py
│   │   └── logging_config.py
│   │
│   ├── models/
│   │   ├── enums.py
│   │   └── message.py
│   │
│   └── storage/
│       └── database.py
│
├── worker/
│   ├── registry.py
│   ├── result.py
│   ├── worker.py
│   └── test_worker.py
│
├── tests/
│   ├── test_broker_dispatcher.py
│   ├── test_broker_integration.py
│   ├── test_broker_lifecycle.py
│   ├── test_broker_models.py
│   ├── test_broker_scheduler.py
│   ├── test_broker_storage.py
│   ├── test_config.py
│   ├── test_database.py
│   ├── test_end_to_end.py
│   ├── test_errors.py
│   ├── test_integration_smoke.py
│   ├── test_logging.py
│   ├── test_peer_integration.py
│   └── test_worker_integration.py
│
├── docs/
├── demo_registration.py
├── demo_dispatcher.py
├── demo_queue.py
├── demo_attempt.py
├── demo_schedular.py
└── README.md
```

## Team Responsibilities

| Member | Responsibility |
|---|---|
| Member 1 | Network and P2P |
| Member 2 | Broker and Scheduling |
| Member 3 | Worker and Async Execution |
| Member 4 | Integration, Storage and Quality |

## Implemented Components

### 1. Broker

- Task registration and validation.
- Task lookup and status management.
- In-memory task queue.
- Submission-key deduplication.
- Integration with scheduler and storage.

### 2. Scheduler

- Least-loaded peer selection.
- Peer health and availability checks.
- Capacity and heartbeat freshness checks.
- Deterministic peer selection.

### 3. Dispatcher

- Task assignment and dispatch workflow.
- Task acknowledgement handling.
- Running and completed task lifecycle updates.
- Rollback and requeue handling on dispatch failures.

### 4. Worker

- Handler registration.
- Synchronous and asynchronous task execution.
- Task result generation.
- Error handling for failed task execution.

### 5. Storage

- SQLite database initialization.
- Database migration tracking.
- Peer and task persistence.
- Task status updates.
- Persistence across database restarts.

### 6. Configuration and Logging

- JSON configuration loading.
- Shared error classes.
- Structured JSON logging.
- Reusable logging configuration.

### 7. Shared Models

- Message and message type definitions.
- Peer and task-related data models.
- Shared interfaces and protocols.

## Installation

Python 3.11 or newer is required.

The core project uses Python's standard library and does not require third-party runtime dependencies.

Clone the repository and navigate to the project directory.

## Running the Registration Demo

```powershell
python demo_registration.py
```

Expected output includes:

```text
Task registered successfully
All registration checks passed.
```

This demo verifies task registration, task ID generation, task retrieval, and basic validation.

## Running Tests

### Complete unittest suite

```powershell
python -m unittest discover -s tests -v
```

### Worker tests

Install pytest if it is not already available:

```powershell
python -m pip install pytest pytest-asyncio
```

Run:

```powershell
python -m pytest worker/test_worker.py -v
```

### Integration smoke test

```powershell
python -m unittest tests.test_integration_smoke -v
```

## Python Syntax Check

```powershell
python -m compileall meshweaver tests worker
```

## Current Verification Results

The following results were verified during the Day 14 mid-review preparation:

| Verification | Result |
|---|---:|
| Registration demo | Passed |
| Integration smoke test | 1/1 passed |
| Complete unittest suite | 77/77 passed |
| Worker pytest suite | 15/15 passed |
| Compile check | Passed |

These results represent the verification run performed during mid-review preparation.

## Project Development Progress

MeshWeaver is being developed in four implementation phases over a 30-day project cycle.

### Week 1 – Foundation (9–15 November 2026)

Focus areas:
- Project architecture and package structure.
- Shared models and protocol contracts.
- Initial SQLite database setup.
- Configuration and error handling.
- Initial module integration and smoke testing.

### Week 2 – Core Integration (16–22 November 2026)

Focus areas:
- Broker, Worker and Storage integration.
- Task lifecycle and persistence verification.
- Integration testing and regression checks.
- Mid-review documentation and demo preparation.
- README and technical documentation updates.

### Week 3 – Reliability (23–29 November 2026)

Planned focus areas:
- Peer failure and reconnection testing.
- Task retries, deadlines and deduplication verification.
- Invalid payload and execution-result validation.
- SQLite persistence recovery testing.
- Multi-peer system integration testing.
- API and protocol documentation improvements.

### Week 4 – Final Integration (30 November–8 December 2026)

Planned focus areas:
- Complete application integration.
- CLI and application lifecycle improvements.
- System regression testing.
- Clean environment installation verification.
- Final documentation and demonstration preparation.

**Development Status:** Week 1 and Week 2 activities have been completed according to the current project progress report. Week 3 focuses on reliability, recovery and integration testing.

Planned features are not considered implemented until their functionality has been verified through source code and appropriate tests.

## Architecture and Networking Status

MeshWeaver includes broker, scheduler, dispatcher, worker, storage, configuration, and shared message components.

The dispatcher uses a transport protocol abstraction. This provides a boundary for communication but does not by itself establish a completed TCP networking implementation.

Actual multi-node TCP communication and distributed execution should be considered separate integration work until verified end-to-end.

The current broker's submission deduplication and dispatcher reservations are local to a Broker instance. The project does not claim distributed exactly-once execution.

## Configuration

The default configuration file is:

```text
meshweaver/config/default_config.json
```

The configuration loader is:

```text
meshweaver/config/loader.py
```

Example:

```python
from meshweaver.config.loader import load_config

config = load_config()
print(config)
```

## Documentation

Project documentation and mid-review evidence are maintained in the `docs/` directory.

## Development Workflow

- Each team member works on an assigned Git branch.
- Changes are integrated through Git and GitHub.
- Pull requests are used for review and integration.
- Automated tests are used to verify component behavior.
- Documentation is maintained alongside implementation changes.

---

**MeshWeaver – Zero-Dependency P2P Async Task Broker**
# MeshWeaver – Week 1–2 Mid-Review Demo Guide

## 1. Overview

This guide describes the demonstration and verification steps for MeshWeaver, a Zero-Dependency P2P Async Task Broker.

The Week 1–2 demonstration covers:

- Task registration and validation.
- Broker task lifecycle and scheduling.
- Peer eligibility checks.
- SQLite persistence.
- Dispatcher integration and task assignment.
- Worker task execution.
- Structured logging and shared models.

## 2. Prerequisites

- Python 3.11 or newer
- Git
- MeshWeaver repository cloned locally

The core application uses Python's standard library.

For the separate Worker test suite, pytest and pytest-asyncio are required.

Install optional testing tools if needed:

```powershell
python -m pip install pytest pytest-asyncio
```

## 3. Run the Broker Registration Demo

From the project root:

```powershell
python demo_registration.py
```

Expected output includes:

```text
Task registered successfully
All registration checks passed.
```

This demonstrates:

- Task registration.
- UUID task ID generation.
- Task retrieval.
- Task details and status inspection.
- Task counting.
- Basic input validation.

## 4. Run the Integration Smoke Test

Execute:

```powershell
python -m unittest tests.test_integration_smoke -v
```

This verifies basic integration involving:

- Configuration loading.
- Broker task submission and retrieval.
- SQLite database initialization.
- Peer persistence.
- Task persistence.

Expected result: `Ran 1 test` and `OK`.

## 5. Run the Complete Test Suite

Execute:

```powershell
python -m unittest discover -s tests -v
```

Verified result during Day 14 preparation:

- 77 tests passed.
- 0 failures.
- 0 errors.

The suite covers Broker, Dispatcher, Scheduler, Storage, Database, Peer integration, Worker integration, Logging, and End-to-End behavior.

## 6. Run the Worker Test Suite

Execute:

```powershell
python -m pytest worker/test_worker.py -v
```

Verified result:

- 15 tests passed.
- 0 failures.

This suite covers synchronous and asynchronous execution, handler registration, input validation, and worker result generation.

## 7. Run the Python Syntax Check

Execute:

```powershell
python -m compileall meshweaver tests worker
```

The syntax check completed successfully during Day 14 preparation.

## 8. Project Modules Demonstrated

| Module | Responsibility |
|---|---|
| `meshweaver/broker` | Task registration, queue, scheduling, dispatch and lifecycle |
| `meshweaver/config` | Configuration loading |
| `meshweaver/models` | Shared messages and protocol types |
| `meshweaver/storage` | SQLite persistence |
| `meshweaver/core` | Shared errors and structured logging |
| `worker` | Handler registration and task execution |
| `tests` | Unit and integration testing |

## 9. Current Implementation Notes

- Broker task registration and lifecycle management are implemented.
- Broker integration with SQLite persistence is covered by automated tests.
- Scheduler selects eligible peers using health, capacity and availability information.
- Dispatcher manages task assignment and lifecycle updates through a transport protocol abstraction.
- Worker supports registered synchronous and asynchronous handlers.
- Database tests verify persistence across database restarts.
- Submission-key deduplication and dispatcher reservations are local to a Broker instance.
- The transport protocol abstraction does not itself prove that actual TCP networking is complete.
- Full distributed exactly-once execution is not guaranteed.

## 10. Week 1–2 Verification Summary

The following results were obtained during the Day 14 verification run:

| Verification | Result |
|---|---:|
| Broker registration demo | Passed |
| Integration smoke test | 1/1 passed |
| Complete unittest suite | 77/77 passed |
| Worker pytest suite | 15/15 passed |
| Python syntax check | Passed |

These are recorded test results, not a claim of completed multi-machine P2P networking.

---

**MeshWeaver – Week 1–2 Mid-Review Demonstration**
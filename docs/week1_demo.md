# MeshWeaver – Week 1 Demo Guide

## 1. Overview

This document describes the Week 1 demonstration steps for MeshWeaver, a Zero-Dependency P2P Async Task Broker.

The demo verifies the basic configuration loader, broker task registration, SQLite persistence, and integration smoke test.

## 2. Prerequisites

- Python 3.10 or newer
- Git
- MeshWeaver repository cloned locally

No third-party runtime dependencies are required for these demonstrations.

## 3. Run the Existing Broker Demo

From the project root, execute:

```powershell
python demo_registration.py
```

Expected result:

```text
Task registered successfully
All registration checks passed.
```

This demonstrates task registration, UUID generation, task retrieval, task counting, and basic input validation.

## 4. Run the Integration Smoke Test

Execute:

```powershell
python -m unittest tests.test_integration_smoke -v
```

This verifies:
- Configuration loading
- Broker task submission and retrieval
- SQLite database initialization
- Peer persistence
- Task persistence

Expected result: `OK`

## 5. Run the Complete Test Suite

Execute:

```powershell
python -m unittest discover -s tests -v
```

Week 1 verification result:

- 12 tests passed
- 0 failures
- 0 errors

## 6. Project Modules Demonstrated

| Module | Responsibility |
|---|---|
| `meshweaver/config` | Configuration loading |
| `meshweaver/models` | Shared protocol models |
| `meshweaver/broker` | Task registration and retrieval |
| `meshweaver/storage` | SQLite persistence |
| `meshweaver/core` | Shared error classes |
| `tests` | Unit and integration tests |

## 7. Current Implementation Notes

- Broker tasks are currently stored in memory.
- SQLite persistence is verified through the integration smoke test.
- Broker-to-database automatic persistence is not yet implemented.
- The integration smoke test verifies basic module interaction, not full peer-to-peer networking or worker execution.

## 8. Week 1 Verification Summary

The configuration, broker registration, SQLite storage, and basic integration workflow have been tested successfully.

Latest verified test result: **12 tests passed, 0 failures.**

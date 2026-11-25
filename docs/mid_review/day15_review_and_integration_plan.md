# Day 15 – Review Findings and Integration Test Plan

**Project:** MeshWeaver – Zero-Dependency P2P Async Task Broker
**Role:** Member 4 – Integration, Storage & Quality
**Review date:** 23 November 2026
**Document status:** Static review completed; runtime verification pending

---

## 1. Review Summary

Day 15 focused on reviewing the available implementation and test snapshots, identifying potential issues, prioritizing improvements, and preparing an integration test plan.

A total of 14 review snapshots were inspected:

1. broker_review.txt
2. dispatcher_review.txt
3. dispatcher_tests_review.txt
4. init_review.txt
5. interfaces_review.txt
6. lifecycle_tests_review.txt
7. models_review.txt
8. models_tests_review.txt
9. peers_review.txt
10. queue_review.txt
11. scheduler_review.txt
12. scheduler_tests_review.txt
13. worker_review.txt
14. worker_test_review.txt

The review was static. The snapshots were inspected, but the current repository test suite was not rerun as part of this review.

Therefore, the findings below are investigation items and potential test gaps, not confirmed runtime defects.

## 2. Prioritized Issue List

Priority definitions:

- P0 – Critical: Serious correctness or functionality blocker.
- P1 – High: Important integration or reliability concern.
- P2 – Medium: Edge cases, validation, or missing test coverage.
- P3 – Low: Cleanup and documentation improvements.

No P0 issue was established during the static review.

### 2.1 P1 – High Priority

| ID | Component | Investigation item | Proposed action |
|---|---|---|---|
| MW-01 | Broker and Queue | Verify task registry and queue consistency during enqueue, dequeue, and asynchronous waiting. | Test lifecycle consistency under failures. |
| MW-02 | Dispatcher | Verify reservation and task-state handling during transport failures, delayed ACKs, and out-of-order messages. | Add dispatch lifecycle integration tests. |
| MW-03 | Persistence | Verify consistency between SQLite state and in-memory task state during failures. | Test persistence, restart, and recovery. |
| MW-04 | Networking Integration | Existing dispatcher tests use FakeTransport and do not demonstrate real TCP/NDJSON integration. | Coordinate with Member 1 and test the actual transport boundary. |

### 2.2 P2 – Medium Priority

| ID | Component | Investigation item | Proposed action |
|---|---|---|---|
| MW-05 | Worker | Task names are validated but not normalized before registration and lookup. | Test surrounding whitespace in task names. |
| MW-06 | Worker | Execution methods accept positional arguments only. | Confirm whether keyword arguments are required by the task contract. |
| MW-07 | Worker Tests | Async pytest tests have no visible markers in the reviewed snapshot. | Verify pytest configuration and actual test execution. |
| MW-08 | Task Models | Previous last_error values may remain after a successful retry. | Confirm intended behaviour and add a test. |
| MW-09 | Peer and Scheduler | Invalid clock values and future heartbeat timestamps need verification. | Add boundary and invalid-input tests. |
| MW-10 | Queue | Duplicate task IDs and multiple waiting consumers need stress testing. | Add concurrent-consumer tests. |
| MW-11 | Dispatcher | Result messages received while a task is ASSIGNED but not yet RUNNING need explicit expected behaviour. | Verify the protocol contract and add a test. |
| MW-12 | Broker | Submission-key deduplication is local to a Broker instance. | Document the limitation and test restart behaviour. |

### 2.3 P3 – Low Priority

| ID | Component | Investigation item | Proposed action |
|---|---|---|---|
| MW-13 | Package Exports | Minor import organization cleanup. | Optional cleanup. |
| MW-14 | Documentation | Clarify Member 1 networking responsibilities and Member 4 broker responsibilities. | Update architecture documentation. |
| MW-15 | Test Evidence | Distinguish unit tests, fake-transport tests, and real system integration tests. | Improve test evidence documentation. |

### 2.4 Priority Summary

- P1: 4 investigation items
- P2: 8 investigation items
- P3: 3 investigation items
- P0: 0 confirmed items

These priorities are provisional and may change after runtime verification.

## 3. Integration Test Plan

The objective is to verify that Broker, Queue, Scheduler, Peer, Dispatcher, Worker, and SQLite work together correctly.

### 3.1 Broker and Queue Integration

| Test ID | Scenario | Expected result |
|---|---|---|
| IT-01 | Register and enqueue a valid task. | Task is registered and queued. |
| IT-02 | Submit multiple tasks. | FIFO ordering is preserved. |
| IT-03 | Submit the same submission key with the same payload. | Existing task is reused according to the deduplication contract. |
| IT-04 | Submit the same submission key with a different payload. | Conflict is rejected. |
| IT-05 | An asynchronous consumer waits for a task. | Consumer receives the next queued task. |
| IT-06 | Consumer is cancelled while waiting. | No queued task is lost. |
| IT-07 | Dispatch fails and the task is requeued. | Task returns to the pending queue without unintended duplication. |

### 3.2 Scheduler and Peer Integration

| Test ID | Scenario | Expected result |
|---|---|---|
| IT-08 | Multiple healthy peers are available. | Least-loaded eligible peer is selected. |
| IT-09 | A peer is offline or unhealthy. | Peer is skipped. |
| IT-10 | A peer has no available capacity. | Peer is skipped. |
| IT-11 | A peer heartbeat becomes stale. | Peer is skipped. |
| IT-12 | A peer is draining. | Peer is skipped. |
| IT-13 | No eligible peers exist. | No peer is selected and the task remains recoverable. |

### 3.3 Dispatcher and Worker Integration

| Test ID | Scenario | Expected result |
|---|---|---|
| IT-14 | Dispatcher assigns a task. | Correct TASK_SUBMIT message is sent. |
| IT-15 | Worker receives a valid task. | Registered handler executes. |
| IT-16 | Worker returns a successful result. | Task reaches COMPLETED. |
| IT-17 | Worker handler raises an exception. | Documented failure and retry policy is followed. |
| IT-18 | Transport send fails. | Reservation is rolled back and task is requeued. |
| IT-19 | Wrong peer sends ACK. | ACK is rejected. |
| IT-20 | Duplicate ACK or result arrives. | No duplicate state transition or reservation release occurs. |
| IT-21 | Result arrives before TASK_STARTED. | Behaviour follows the agreed protocol contract. |
| IT-22 | Task exceeds maximum attempts. | Task reaches FAILED and is no longer dispatched. |

### 3.4 SQLite Persistence and Recovery

| Test ID | Scenario | Expected result |
|---|---|---|
| IT-23 | Register a task with SQLite enabled. | Task is persisted. |
| IT-24 | Update task lifecycle state. | Database reflects the state change. |
| IT-25 | Close and reopen the database. | Persisted task data remains available. |
| IT-26 | Broker restarts after task registration. | Recovery follows the documented persistence contract. |
| IT-27 | Database operation fails during a lifecycle update. | In-memory and database inconsistency is detected and handled. |
| IT-28 | Retry and terminal state are persisted. | Correct final state survives database reopening. |

### 3.5 End-to-End System Test

Proposed workflow:

1. Client submits a task to the broker.
2. Broker registers and queues the task.
3. Scheduler selects an eligible worker peer.
4. Dispatcher sends the task to the selected peer.
5. Worker executes the registered handler.
6. Dispatcher processes the result and updates task state.
7. SQLite stores the final task state.
8. The final task status is verified as COMPLETED.

Initially, this workflow can be tested using a fake transport.

A true multi-peer TCP/NDJSON end-to-end test requires the actual networking implementation supplied by Member 1.

## 4. Test Execution and Acceptance Criteria

For every integration test, record:

- Test ID
- Input and scenario
- Expected result
- Actual result
- PASS or FAIL
- Related issue ID, if applicable

The integration plan is considered validated when:

1. All P1 items have an owner or documented dependency.
2. Integration tests have clear expected outcomes.
3. Unit-test evidence is distinguished from integration-test evidence.
4. Networking dependencies are coordinated with Member 1.
5. Unverified findings are not reported as confirmed defects.
6. Persistence and recovery behaviour is verified against the actual implementation.

## 5. Ownership and Dependencies

### Member 4 – Integration, Storage and Quality

- Broker and task lifecycle integration.
- Queue and scheduler integration.
- SQLite persistence and recovery verification.
- Integration and regression testing.
- Review documentation and test evidence.
- Coordination of cross-module integration.

### Member 1 – Networking

- TCP networking.
- NDJSON message transport.
- Heartbeat exchange.
- Peer discovery.
- Real network integration support.

Real network tests depend on the availability of Member 1's networking implementation.

## 6. Review Limitations

The following limitations must be considered:

1. Review was based on 14 source-code and test snapshots.
2. The current repository test suite was not rerun during this static review.
3. Potential issues were not all verified against the live source files.
4. FakeTransport tests do not prove actual TCP/NDJSON communication.
5. Local submission-key deduplication does not establish distributed exactly-once execution.
6. Local dispatcher reservations do not establish cluster-wide capacity coordination.
7. SQLite restart and recovery guarantees require dedicated runtime testing.

## 7. Day 15 Completion Record

**Day:** 15
**Planned focus:** Review feedback, prioritized issue list, and integration test plan.

Completed activities:

- Reviewed 14 implementation and test snapshots.
- Compared implementation behaviour with available test coverage.
- Prepared 15 prioritized investigation and improvement items.
- Prepared 28 integration test scenarios.
- Identified Member 1 networking dependencies.
- Documented runtime verification requirements.
- Prepared acceptance criteria for the next integration phase.

Pending activities:

- Rerun the current repository test suite.
- Verify potential issues against live source code.
- Execute integration and persistence recovery tests.
- Assign owners and track issue resolution.
- Coordinate real networking tests with Member 1.

**Day 15 review and planning deliverables: PREPARED.**

**Note:** Documentation preparation does not imply that pending runtime tests or integration work have been completed.

---

**Suggested commit message:**

`docs: add day 15 review findings and integration test plan`

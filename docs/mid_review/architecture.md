**# MeshWeaver – System Architecture**



**\*\*Project:\*\* MeshWeaver – Zero-Dependency P2P Async Task Broker**  

**\*\*Documentation:\*\* Mid-Review Architecture**  

**\*\*Team Role:\*\* Member 4 – Integration, Storage \& Quality**  

**\*\*Review Period:\*\* 9 November – 22 November 2026**



**---**



**## 1. System Overview**



**MeshWeaver is a Zero-Dependency Peer-to-Peer Asynchronous Task Broker.**



**Its architecture separates task management, scheduling, dispatching, peer information, storage, shared message models and communication interfaces into independent modules.**



**The design uses Python interfaces and protocols to establish integration boundaries between components.**



**## 2. Main Components**



**| Component | Responsibility |**

**|---|---|**

**| Broker | Task registration, validation, lookup and queue management |**

**| Task Queue | Maintains pending task IDs |**

**| Scheduler | Selects eligible worker peers |**

**| Dispatcher | Assigns tasks and manages dispatch lifecycle |**

**| PeerInfo | Maintains worker peer snapshots and eligibility information |**

**| SQLite Database | Stores peer and task information |**

**| Message Model | Defines the shared message structure |**

**| Interfaces | Defines communication contracts between modules |**

**| Configuration Loader | Loads application settings from JSON |**

**| Logging | Provides structured JSON logging |**



**## 3. High-Level Architecture Diagram**



**```mermaid**

**flowchart TD**

&#x20;   **A\[Task Submission] --> B\[Broker]**

&#x20;   **B --> C\[Task Validation]**

&#x20;   **C --> D\[Task Queue]**

&#x20;   **B --> E\[SQLite Storage]**

&#x20;   **D --> F\[Dispatcher]**

&#x20;   **F --> G\[Scheduler]**

&#x20;   **G --> H\[Peer Information]**

&#x20;   **H --> G**

&#x20;   **G --> F**

&#x20;   **F --> I\[Task Transport Protocol]**

&#x20;   **I --> J\[Network Transport Integration Boundary]**

&#x20;   **J --> K\[Worker Peer]**

&#x20;   **K --> J**

&#x20;   **J --> I**

&#x20;   **I --> F**

&#x20;   **F --> B**

**```**



**\*\*Diagram explanation:\*\***



**1. A task is submitted to the Broker.**

**2. Broker validates and registers the task.**

**3. Task information can be persisted in SQLite.**

**4. Pending task IDs are maintained by the queue.**

**5. Dispatcher requests an eligible peer from Scheduler.**

**6. Scheduler evaluates peer health and available capacity.**

**7. Dispatcher uses the transport protocol as the communication boundary.**



**\*\*Important:\*\* The diagram represents logical component relationships. It does not claim that a complete TCP networking implementation has been verified.**



**## 4. Task Dispatch Lifecycle**



**```mermaid**

**flowchart TD**

&#x20;   **A\[Task Registered] --> B\[Queued]**

&#x20;   **B --> C\[Dispatcher Requests Task]**

&#x20;   **C --> D\[Scheduler Selects Peer]**

&#x20;   **D --> E{Eligible Peer Found?}**

&#x20;   **E -->|No| B**

&#x20;   **E -->|Yes| F\[ASSIGNING]**

&#x20;   **F --> G\[Send TASK\_SUBMIT]**

&#x20;   **G --> H{Acknowledgement?}**

&#x20;   **H -->|Accepted| I\[ASSIGNED]**

&#x20;   **H -->|Dispatch Failure| J\[Rollback and Requeue]**

&#x20;   **J --> B**

&#x20;   **I --> K\[TASK\_STARTED]**

&#x20;   **K --> L\[RUNNING]**

&#x20;   **L --> M\[TASK\_RESULT]**

&#x20;   **M --> N\[COMPLETED]**

**```**



**The inspected Dispatcher implementation supports assignment, acknowledgement handling, task-start handling, result handling and rollback on dispatch failure.**



**## 5. Peer Scheduling Architecture**



**The Scheduler uses `PeerInfo` snapshots to identify eligible worker peers.**



**Eligibility checks include:**



**- Peer is online.**

**- Peer is not draining.**

**- Peer health is `HEALTHY`.**

**- Peer has available task capacity.**

**- Heartbeat freshness is checked when configured.**



**The inspected `LeastLoadedScheduler` considers CPU usage, running task count and peer ID when ordering eligible peers.**



**## 6. Storage Architecture**



**The project uses SQLite through the `Database` class.**



**Two main tables are defined:**



**### Peers Table**



**- `peer\_id`**

**- `host`**

**- `port`**

**- `status`**

**- `last\_seen`**

**- `created\_at`**



**### Tasks Table**



**- `task\_id`**

**- `task\_type`**

**- `payload`**

**- `status`**

**- `assigned\_peer\_id`**

**- `result`**

**- `created\_at`**

**- `updated\_at`**



**The database module provides CRUD operations for peers and tasks.**



**Schema initialization uses a migration tracking table named `schema\_migrations`.**



**## 7. Shared Message Architecture**



**The shared `Message` model contains:**



**- `message\_id`**

**- `message\_type`**

**- `sender\_id`**

**- `payload`**

**- `timestamp`**

**- `version`**



**It supports dictionary conversion through:**



**- `to\_dict()`**

**- `from\_dict()`**



**Supported message types include peer communication, task lifecycle and status request messages.**



**## 8. Interface and Integration Boundaries**



**The broker interfaces module defines four protocols:**



**1. `TaskQueueProtocol`**

**2. `BrokerProtocol`**

**3. `PeerSelectorProtocol`**

**4. `TaskTransportProtocol`**



**These contracts allow components to interact without depending directly on every implementation.**



**\*\*Transport ownership:\*\* Member 1 owns the real asyncio TCP/NDJSON transport. The inspected `TaskTransportProtocol` defines the interface used by the broker-side dispatcher.**



**## 9. Configuration and Logging**



**### Configuration**



**The default JSON configuration contains:**



**- Node ID**

**- Host and port**

**- Peer list**

**- Database path**

**- Maximum concurrent tasks**

**- Heartbeat interval**

**- Maximum message size**

**- Protocol version**



**The configuration loader reads JSON and reports missing files or invalid JSON through `ConfigurationError`.**



**### Logging**



**The logging module provides:**



**- Centralized MeshWeaver logger**

**- Structured JSON formatting**

**- UTC timestamps**

**- Log levels and logger names**

**- Exception information**



**## 10. Architecture Limitations and Future Integration**



**The following points must be considered during further development:**



**- Actual TCP/NDJSON transport integration must be verified separately.**

**- The transport protocol alone does not establish complete peer-to-peer communication.**

**- Dispatcher reservations are local to a broker instance.**

**- Submission-key deduplication is local to a broker instance.**

**- Distributed exactly-once execution is not guaranteed.**

**- Additional recovery and multi-peer reliability testing is required.**



**---**



**\*\*Prepared for:\*\* MeshWeaver Mid-Review**  

**\*\*Prepared by:\*\* Member 4 – Integration, Storage \& Quality**




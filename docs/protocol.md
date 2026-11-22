# MeshWeaver Shared Communication Protocol

## 1. Overview

MeshWeaver peers exchange messages using JSON.

Messages are designed for newline-delimited JSON (NDJSON)
communication over TCP. Each message is represented as a
JSON object and separated by a newline.

The shared message structure ensures consistent communication
between different MeshWeaver components and team members.

## 2. Message Structure

Every message follows the shared structure:

| Field | Description |
|---|---|
| `message_id` | Unique identifier for the message. |
| `message_type` | Type of operation being performed. |
| `sender_id` | Identifier of the sending peer. |
| `payload` | Message-specific data represented as a JSON object. |
| `timestamp` | UTC message creation timestamp in ISO format. |
| `version` | Protocol version used by the message. |

### Message Contract

- `message_id` uniquely identifies a message.
- `message_type` must use one of the supported message types.
- `sender_id` identifies the peer sending the message.
- `payload` contains data specific to the message type.
- `timestamp` represents the message creation time in UTC.
- `version` identifies the protocol version. The current
  message model uses version `1`.

The message model provides serialization and deserialization
through `to_dict()` and `from_dict()`.

## 3. Supported Message Types

| Message Type | Purpose |
|---|---|
| `HELLO` | Initial peer introduction. |
| `HEARTBEAT` | Peer availability signal. |
| `PEER_LIST` | Exchange peer information. |
| `TASK_SUBMIT` | Submit a task. |
| `TASK_ACK` | Acknowledge task receipt. |
| `TASK_REJECT` | Reject a task. |
| `TASK_STARTED` | Report task execution start. |
| `TASK_RESULT` | Return successful task output. |
| `TASK_FAILED` | Report task execution failure. |
| `STATUS_REQUEST` | Request node status. |

All message types must use the names defined in the shared
`MessageType` enum.

New message types must not be introduced independently
without discussion and agreement between team members.

## 4. Message Exchange Format

MeshWeaver uses JSON messages with NDJSON framing over TCP.

Each message must be represented as a JSON object and
terminated by a newline delimiter.

The message envelope remains consistent across all message
types. Only the contents of the `payload` field vary according
to the operation.

Specific payload structures should be agreed upon by the
relevant team members before implementation to maintain
compatibility.

## 5. Configuration Fields

| Field | Description |
|---|---|
| `node_id` | Unique node identifier. |
| `host` | Network host address. |
| `port` | Network listening port. |
| `peers` | Known peer addresses. |
| `database_path` | SQLite database location. |
| `max_concurrent_tasks` | Maximum concurrent tasks. |
| `heartbeat_interval` | Heartbeat interval in seconds. |
| `max_message_bytes` | Maximum message size. |
| `protocol_version` | Protocol version. |

## 6. Team Contract

- All members must use the shared message structure.
- Message types must follow the shared `MessageType` enum.
- The message envelope fields must remain consistent.
- Payload structures should be coordinated between the
  members implementing the sender and receiver.
- New message types or changes to existing fields must be
  discussed before implementation.
- Changes must maintain compatibility across project
  components.

## 7. Implementation Boundary

The shared message model defines the message structure and
JSON serialization/deserialization contract.

The protocol documentation describes the intended
communication format. Defining this contract does not,
by itself, mean that TCP message exchange or complete
peer-to-peer task dispatch has been implemented.
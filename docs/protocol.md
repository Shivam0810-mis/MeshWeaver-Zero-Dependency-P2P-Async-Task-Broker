
# MeshWeaver Shared Communication Protocol

## Overview
MeshWeaver peers exchange messages using JSON.
Messages are designed for newline-delimited JSON (NDJSON)
communication over TCP.

## Message Structure

Every message contains:

- message_id: Unique identifier for the message.
- message_type: Type of operation being performed.
- sender_id: Identifier of the sending peer.
- payload: Message-specific data.
- timestamp: UTC message creation time.
- version: Protocol version.

## Supported Message Types

- HELLO: Initial peer introduction.
- HEARTBEAT: Peer availability signal.
- PEER_LIST: Exchange peer information.
- TASK_SUBMIT: Submit a task.
- TASK_ACK: Acknowledge task receipt.
- TASK_REJECT: Reject a task.
- TASK_STARTED: Report task execution start.
- TASK_RESULT: Return successful task output.
- TASK_FAILED: Report task execution failure.
- STATUS_REQUEST: Request node status.

## Configuration Fields

- node_id: Unique node identifier.
- host: Network host address.
- port: Network listening port.
- peers: Known peer addresses.
- database_path: SQLite database location.
- max_concurrent_tasks: Maximum concurrent tasks.
- heartbeat_interval: Heartbeat interval in seconds.
- max_message_bytes: Maximum message size.
- protocol_version: Protocol version.

## Team Contract

All members should use the shared message structure.
New message types or field changes should be discussed
before implementation to maintain compatibility.

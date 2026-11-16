
from enum import Enum


class MessageType(str, Enum):
    """Supported message types exchanged between MeshWeaver peers."""

    HELLO = "HELLO"
    HEARTBEAT = "HEARTBEAT"
    PEER_LIST = "PEER_LIST"

    TASK_SUBMIT = "TASK_SUBMIT"
    TASK_ACK = "TASK_ACK"
    TASK_REJECT = "TASK_REJECT"
    TASK_STARTED = "TASK_STARTED"
    TASK_RESULT = "TASK_RESULT"
    TASK_FAILED = "TASK_FAILED"

    STATUS_REQUEST = "STATUS_REQUEST"

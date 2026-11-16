
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from meshweaver.models.enums import MessageType


@dataclass
class Message:
    """Standard message exchanged between MeshWeaver nodes."""

    message_type: MessageType
    sender_id: str
    payload: dict[str, Any] = field(default_factory=dict)
    message_id: str = field(default_factory=lambda: str(uuid4()))
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    version: int = 1

    def __post_init__(self) -> None:
        if not isinstance(self.message_type, MessageType):
            self.message_type = MessageType(self.message_type)

        if not self.sender_id or not isinstance(self.sender_id, str):
            raise ValueError("sender_id must be a non-empty string")

        if not isinstance(self.payload, dict):
            raise ValueError("payload must be a dictionary")

        if not self.message_id:
            raise ValueError("message_id cannot be empty")

        if self.version != 1:
            raise ValueError("Unsupported message version")

    def to_dict(self) -> dict[str, Any]:
        """Convert the message into a JSON-compatible dictionary."""
        return {
            "message_id": self.message_id,
            "message_type": self.message_type.value,
            "sender_id": self.sender_id,
            "payload": self.payload,
            "timestamp": self.timestamp,
            "version": self.version,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Message":
        """Create a Message object from a received dictionary."""
        required_fields = {
            "message_type",
            "sender_id",
        }

        if not isinstance(data, dict):
            raise ValueError("Message data must be a dictionary")

        missing_fields = required_fields - data.keys()
        if missing_fields:
            raise ValueError(
                f"Missing message fields: {', '.join(sorted(missing_fields))}"
            )

        try:
            return cls(
                message_type=MessageType(data["message_type"]),
                sender_id=data["sender_id"],
                payload=data.get("payload", {}),
                message_id=data.get("message_id", str(uuid4())),
                timestamp=data.get(
                    "timestamp",
                    datetime.now(timezone.utc).isoformat(),
                ),
                version=data.get("version", 1),
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid message data: {exc}") from exc

"""Peer snapshot model and health/staleness eligibility checks for scheduling.

Member 1 owns TCP networking, heartbeat exchange, and peer discovery.
``PeerInfo`` is the broker-side snapshot consumed by scheduling policies.
"""

from dataclasses import dataclass
from enum import Enum
from math import isfinite
from typing import Optional

from .models import TaskValidationError


class PeerHealth(str, Enum):
    """Health classification reported for a worker peer."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    OFFLINE = "offline"


@dataclass(frozen=True)
class PeerInfo:
    """Immutable worker peer snapshot used for scheduling decisions."""

    peer_id: str
    cpu_usage: float
    running_tasks: int = 0
    max_tasks: int = 1
    is_online: bool = True
    health: PeerHealth = PeerHealth.HEALTHY
    last_seen_monotonic: Optional[float] = None
    is_draining: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.peer_id, str) or not self.peer_id.strip():
            raise TaskValidationError("Peer ID cannot be empty.")

        if (
            isinstance(self.cpu_usage, bool)
            or not isinstance(self.cpu_usage, (int, float))
            or not isfinite(self.cpu_usage)
            or not 0 <= self.cpu_usage <= 100
        ):
            raise TaskValidationError(
                "CPU usage must be a finite number from 0 to 100."
            )

        if (
            isinstance(self.running_tasks, bool)
            or not isinstance(self.running_tasks, int)
            or self.running_tasks < 0
        ):
            raise TaskValidationError("Running tasks cannot be negative.")

        if (
            isinstance(self.max_tasks, bool)
            or not isinstance(self.max_tasks, int)
            or self.max_tasks < 1
        ):
            raise TaskValidationError("Max tasks must be at least 1.")

        if not isinstance(self.is_online, bool):
            raise TaskValidationError("is_online must be a boolean.")

        if not isinstance(self.is_draining, bool):
            raise TaskValidationError("is_draining must be a boolean.")

        if not isinstance(self.health, PeerHealth):
            try:
                object.__setattr__(self, "health", PeerHealth(self.health))
            except ValueError as exc:
                raise TaskValidationError(
                    f"Invalid peer health status: {self.health!r}"
                ) from exc

        if self.last_seen_monotonic is not None:
            if (
                isinstance(self.last_seen_monotonic, bool)
                or not isinstance(self.last_seen_monotonic, (int, float))
                or not isfinite(self.last_seen_monotonic)
                or self.last_seen_monotonic < 0
            ):
                raise TaskValidationError(
                    "last_seen_monotonic must be a non-negative finite number."
                )

    @property
    def available_slots(self) -> int:
        """Return remaining local task slots on this peer snapshot."""
        return max(0, self.max_tasks - self.running_tasks)

    def is_stale(
        self,
        now_monotonic: float,
        max_staleness_seconds: float,
    ) -> bool:
        """Return True if ``last_seen_monotonic`` exceeds staleness window."""
        if self.last_seen_monotonic is None:
            return False
        return (now_monotonic - self.last_seen_monotonic) > max_staleness_seconds

    def is_eligible(
        self,
        *,
        now_monotonic: Optional[float] = None,
        max_staleness_seconds: Optional[float] = None,
        require_heartbeat: bool = False,
    ) -> bool:
        """Return True if this peer is online, healthy, fresh, and has capacity."""
        if not self.is_online:
            return False
        if self.is_draining:
            return False
        if self.health != PeerHealth.HEALTHY:
            return False
        if self.running_tasks >= self.max_tasks:
            return False

        if max_staleness_seconds is not None:
            if self.last_seen_monotonic is None:
                if require_heartbeat:
                    return False
            else:
                if now_monotonic is None:
                    raise TaskValidationError(
                        "now_monotonic is required when evaluating peer staleness."
                    )
                if self.is_stale(now_monotonic, max_staleness_seconds):
                    return False

        return True

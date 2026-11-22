"""Scheduling policy selecting eligible, healthy, least-loaded peers."""

import time
from math import isfinite
from typing import Callable, Iterable, List, Optional

from .models import TaskValidationError
from .peers import PeerInfo


class LeastLoadedScheduler:
    """Selects the least-loaded eligible, healthy, non-stale peer.

    Eligibility rules:
        - Peer is online (``is_online=True``) and not draining.
        - Peer reports ``health == PeerHealth.HEALTHY``.
        - Peer has available capacity (``running_tasks < max_tasks``).
        - If ``max_staleness_seconds`` is set and the peer carries a
          ``last_seen_monotonic`` timestamp (or ``require_heartbeat=True``),
          the heartbeat must be within ``max_staleness_seconds`` of the
          injected monotonic clock.

    Selection tie-breaking order:
        1. Lowest reported ``cpu_usage``
        2. Lowest ``running_tasks``
        3. Lexicographically smallest ``peer_id`` (deterministic)
    """

    def __init__(
        self,
        *,
        max_staleness_seconds: Optional[float] = 90.0,
        require_heartbeat: bool = False,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if max_staleness_seconds is not None:
            if (
                isinstance(max_staleness_seconds, bool)
                or not isinstance(max_staleness_seconds, (int, float))
                or not isfinite(max_staleness_seconds)
                or max_staleness_seconds < 0
            ):
                raise TaskValidationError(
                    "max_staleness_seconds must be a non-negative finite number or None."
                )

        self._max_staleness_seconds = max_staleness_seconds
        self._require_heartbeat = require_heartbeat
        self._clock = clock

    def is_peer_eligible(
        self,
        peer: PeerInfo,
        *,
        now_monotonic: Optional[float] = None,
    ) -> bool:
        """Evaluate whether ``peer`` satisfies health, freshness, and capacity rules."""
        now = self._clock() if now_monotonic is None else now_monotonic
        return peer.is_eligible(
            now_monotonic=now,
            max_staleness_seconds=self._max_staleness_seconds,
            require_heartbeat=self._require_heartbeat,
        )

    def filter_eligible_peers(
        self,
        peers: Iterable[PeerInfo],
        *,
        now_monotonic: Optional[float] = None,
    ) -> List[PeerInfo]:
        """Return all eligible peers evaluated against one consistent timestamp."""
        now = self._clock() if now_monotonic is None else now_monotonic
        return [
            peer
            for peer in peers
            if self.is_peer_eligible(peer, now_monotonic=now)
        ]

    def select_peer(
        self,
        peers: Iterable[PeerInfo],
        *,
        now_monotonic: Optional[float] = None,
    ) -> Optional[PeerInfo]:
        """Return the best eligible peer, or None if no peer qualifies."""
        eligible_peers = self.filter_eligible_peers(
            peers,
            now_monotonic=now_monotonic,
        )

        return min(
            eligible_peers,
            key=lambda peer: (
                peer.cpu_usage,
                peer.running_tasks,
                peer.peer_id,
            ),
            default=None,
        )


# Backward-compatible aliases (kept so existing imports stay intact).
Scheduler = LeastLoadedScheduler
scheduler = LeastLoadedScheduler

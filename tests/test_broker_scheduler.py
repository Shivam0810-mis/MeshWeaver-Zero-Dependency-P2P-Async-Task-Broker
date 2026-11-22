"""Unit tests for PeerInfo and LeastLoadedScheduler using fake clocks.

Integration boundary note:
    These tests use in-memory PeerInfo snapshots and a deterministic fake
    monotonic clock to verify broker scheduling policy in isolation. Member 1
    owns real TCP heartbeats and live peer discovery.
"""

import unittest

from meshweaver.broker import (
    LeastLoadedScheduler,
    PeerHealth,
    PeerInfo,
    PeerSelectorProtocol,
    Scheduler,
    TaskValidationError,
)
from meshweaver.broker.scheduler import scheduler as legacy_scheduler_alias


class FakeClock:
    def __init__(self, initial: float = 100.0) -> None:
        self.now = initial

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class TestPeerInfoValidation(unittest.TestCase):
    def test_valid_peer_and_string_health_coercion(self):
        peer = PeerInfo(
            peer_id="peer-1",
            cpu_usage=25.5,
            running_tasks=1,
            max_tasks=4,
            health="degraded",  # type: ignore[arg-type]
            last_seen_monotonic=120.0,
        )
        self.assertEqual(peer.health, PeerHealth.DEGRADED)
        self.assertEqual(peer.available_slots, 3)

    def test_invalid_peer_fields_rejected(self):
        with self.assertRaises(TaskValidationError):
            PeerInfo("   ", 10)
        with self.assertRaises(TaskValidationError):
            PeerInfo("peer-1", 101)
        with self.assertRaises(TaskValidationError):
            PeerInfo("peer-1", float("nan"))
        with self.assertRaises(TaskValidationError):
            PeerInfo("peer-1", True)  # type: ignore[arg-type]
        with self.assertRaises(TaskValidationError):
            PeerInfo("peer-1", 10, running_tasks=-1)
        with self.assertRaises(TaskValidationError):
            PeerInfo("peer-1", 10, running_tasks=True)  # type: ignore[arg-type]
        with self.assertRaises(TaskValidationError):
            PeerInfo("peer-1", 10, max_tasks=0)
        with self.assertRaises(TaskValidationError):
            PeerInfo("peer-1", 10, health="unknown")  # type: ignore[arg-type]
        with self.assertRaises(TaskValidationError):
            PeerInfo("peer-1", 10, last_seen_monotonic=-5.0)


class TestLeastLoadedScheduler(unittest.TestCase):
    def test_conforms_to_protocol_and_preserves_aliases(self):
        sched = LeastLoadedScheduler()
        self.assertIsInstance(sched, PeerSelectorProtocol)
        self.assertIs(Scheduler, LeastLoadedScheduler)
        self.assertIs(legacy_scheduler_alias, LeastLoadedScheduler)

    def test_selects_lowest_cpu_then_running_tasks_then_peer_id(self):
        sched = LeastLoadedScheduler()

        peers = [
            PeerInfo("peer-c", cpu_usage=20, running_tasks=1, max_tasks=4),
            PeerInfo("peer-b", cpu_usage=20, running_tasks=0, max_tasks=4),
            PeerInfo("peer-a", cpu_usage=20, running_tasks=0, max_tasks=4),
            PeerInfo("peer-busy", cpu_usage=5, running_tasks=2, max_tasks=2),
        ]
        selected = sched.select_peer(peers)
        self.assertIsNotNone(selected)
        self.assertEqual(selected.peer_id, "peer-a")

    def test_filters_offline_unhealthy_degraded_draining_and_full_peers(self):
        sched = LeastLoadedScheduler()
        peers = [
            PeerInfo("peer-offline", 1, 0, 4, is_online=False),
            PeerInfo("peer-unhealthy", 2, 0, 4, health=PeerHealth.UNHEALTHY),
            PeerInfo("peer-degraded", 3, 0, 4, health=PeerHealth.DEGRADED),
            PeerInfo("peer-draining", 4, 0, 4, is_draining=True),
            PeerInfo("peer-full", 5, 4, 4),
            PeerInfo("peer-healthy", 60, 1, 4, health=PeerHealth.HEALTHY),
        ]

        selected = sched.select_peer(peers)
        self.assertIsNotNone(selected)
        self.assertEqual(selected.peer_id, "peer-healthy")

    def test_filters_stale_peers_using_fake_clock(self):
        clock = FakeClock(initial=100.0)
        sched = LeastLoadedScheduler(
            max_staleness_seconds=30.0,
            require_heartbeat=True,
            clock=clock,
        )

        peers = [
            PeerInfo("peer-no-hb", 1, 0, 4, last_seen_monotonic=None),
            PeerInfo("peer-fast", 10, 0, 4, last_seen_monotonic=100.0),
            PeerInfo("peer-backup", 40, 0, 4, last_seen_monotonic=115.0),
        ]

        # At t=100, peer-no-hb is excluded (require_heartbeat=True); peer-fast wins.
        selected = sched.select_peer(peers)
        self.assertIsNotNone(selected)
        self.assertEqual(selected.peer_id, "peer-fast")

        # Exact boundary t=130 (age 30.0s <= 30.0s): peer-fast still eligible.
        clock.advance(30.0)
        self.assertEqual(sched.select_peer(peers).peer_id, "peer-fast")

        # At t=131 (age 31.0s > 30.0s): peer-fast is stale, peer-backup wins.
        clock.advance(1.0)
        selected = sched.select_peer(peers)
        self.assertIsNotNone(selected)
        self.assertEqual(selected.peer_id, "peer-backup")

        # At t=146 (all heartbeats stale): returns None.
        clock.advance(15.0)
        self.assertIsNone(sched.select_peer(peers))

    def test_invalid_staleness_config_rejected(self):
        with self.assertRaises(TaskValidationError):
            LeastLoadedScheduler(max_staleness_seconds=-1.0)
        with self.assertRaises(TaskValidationError):
            LeastLoadedScheduler(max_staleness_seconds=True)  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()

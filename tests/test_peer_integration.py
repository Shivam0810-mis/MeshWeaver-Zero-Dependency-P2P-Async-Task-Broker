
import unittest
from dataclasses import FrozenInstanceError

from meshweaver.broker.peers import PeerInfo


class TestPeerIntegration(unittest.TestCase):

    def test_create_valid_peer(self):
        peer = PeerInfo(
            peer_id="peer-1",
            cpu_usage=20,
            running_tasks=0,
            max_tasks=5,
            is_online=True,
        )

        self.assertEqual(peer.peer_id, "peer-1")
        self.assertEqual(peer.cpu_usage, 20)
        self.assertTrue(peer.is_online)

    def test_reject_empty_peer_id(self):
        with self.assertRaises(ValueError):
            PeerInfo(
                peer_id="",
                cpu_usage=20,
            )

    def test_reject_invalid_cpu_usage(self):
        for cpu_usage in (-1, 101, float("inf"), float("nan")):
            with self.subTest(cpu_usage=cpu_usage):
                with self.assertRaises(ValueError):
                    PeerInfo(
                        peer_id="peer-1",
                        cpu_usage=cpu_usage,
                    )

    def test_reject_negative_running_tasks(self):
        with self.assertRaises(ValueError):
            PeerInfo(
                peer_id="peer-1",
                cpu_usage=20,
                running_tasks=-1,
            )

    def test_reject_invalid_max_tasks(self):
        with self.assertRaises(ValueError):
            PeerInfo(
                peer_id="peer-1",
                cpu_usage=20,
                max_tasks=0,
            )

    def test_peer_is_immutable(self):
        peer = PeerInfo(
            peer_id="peer-1",
            cpu_usage=20,
        )

        with self.assertRaises(FrozenInstanceError):
            peer.cpu_usage = 50


if __name__ == "__main__":
    unittest.main()

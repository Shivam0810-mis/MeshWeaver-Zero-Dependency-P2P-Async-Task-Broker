from typing import Iterable, Optional

from .peers import PeerInfo


class scheduler:
    def select_peer(
        self,
        peers: Iterable[PeerInfo],
    ) -> Optional[PeerInfo]:
        eligible_peers = (
            peer
            for peer in peers
            if peer.is_online
            and peer.running_tasks < peer.max_tasks
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
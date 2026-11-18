from meshweaver.broker.peers import PeerInfo
from meshweaver.broker.scheduler import scheduler as Scheduler


def main():
    scheduler = Scheduler()

    peers = [
        PeerInfo("peer-a", 65, 1, 3),
        PeerInfo("peer-b", 20, 0, 3),
        PeerInfo("peer-c", 5, 0, 3, is_online=False),
    ]

    selected = scheduler.select_peer(peers)

    assert selected is not None
    assert selected.peer_id == "peer-b"
    print("Lowest-CPU online peer selected: PASS")

    peers = [
        PeerInfo("peer-full", 2, 2, 2),
        PeerInfo("peer-free", 40, 0, 2),
    ]

    selected = scheduler.select_peer(peers)

    assert selected is not None
    assert selected.peer_id == "peer-free"
    print("Full worker ignored: PASS")

    peers = [
        PeerInfo("peer-a", 30, 2, 4),
        PeerInfo("peer-b", 30, 1, 4),
    ]

    selected = scheduler.select_peer(peers)

    assert selected is not None
    assert selected.peer_id == "peer-b"
    print("CPU tie resolved using running tasks: PASS")

    assert scheduler.select_peer([]) is None
    print("Empty peer list handled: PASS")

    peers = [
        PeerInfo("peer-offline", 10, 0, 2, is_online=False),
        PeerInfo("peer-full", 10, 2, 2),
    ]

    assert scheduler.select_peer(peers) is None
    print("No eligible peer handled: PASS")

    try:
        PeerInfo("peer-invalid", 150)
    except ValueError:
        pass
    else:
        raise AssertionError("Invalid CPU usage was accepted.")

    print("Invalid CPU usage rejected: PASS")
    print("\nAll Step 3 checks passed.")


if __name__ == "__main__":
    main()
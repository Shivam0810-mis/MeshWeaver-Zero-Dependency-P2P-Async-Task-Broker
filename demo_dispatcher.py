from meshweaver.broker.dispatcher import DispatchBroker
from meshweaver.broker.models import TaskStatus
from meshweaver.broker.peers import PeerInfo


def main():
    broker = DispatchBroker()

    first = broker.submit_task("add_numbers", 10, 20)
    second = broker.submit_task("multiply_numbers", 3, 4)

    peers = [
        PeerInfo("peer-a", 10, 0, 1),
        PeerInfo("peer-b", 30, 0, 1),
    ]

    a1 = broker.assign_next(peers)
    assert a1 is not None
    assert a1.peer_id == "peer-a"
    assert first.status == TaskStatus.ASSIGNING
    assert broker.reservation_count("peer-a") == 1
    print("First assignment:", a1.peer_id)

    a2 = broker.assign_next(peers)
    assert a2 is not None
    assert a2.peer_id == "peer-b"
    assert second.status == TaskStatus.ASSIGNING
    print("Second assignment:", a2.peer_id)

    third = broker.submit_task("subtract_numbers", 9, 2)
    assert broker.assign_next(peers) is None
    assert broker.pending_count() == 1
    assert third.status == TaskStatus.QUEUED
    print("No capacity: task remains queued")


    assert not broker.acknowledge(a1.assignment_id, "peer-b")
    assert first.status == TaskStatus.ASSIGNING

    assert broker.acknowledge(a1.assignment_id, "peer-a")
    assert first.status == TaskStatus.ASSIGNED

    assert broker.acknowledge(a1.assignment_id, "peer-a")
    assert broker.reservation_count("peer-a") == 1
    print("ACK validation and duplicate ACK: PASS")

    assert broker.mark_running(a1.assignment_id, "peer-a")
    assert first.status == TaskStatus.RUNNING

    assert broker.acknowledge(a1.assignment_id, "peer-a")
    assert first.status == TaskStatus.RUNNING

    assert broker.complete(a1.assignment_id, "peer-a")
    assert first.status == TaskStatus.COMPLETED
    assert broker.reservation_count("peer-a") == 0

    assert not broker.complete(a1.assignment_id, "peer-a")
    assert broker.reservation_count("peer-a") == 0
    print("Completion releases capacity exactly once: PASS")

    a3 = broker.assign_next(peers)
    assert a3 is not None
    assert a3.task_id == third.task_id
    assert a3.peer_id == "peer-a"
    assert broker.pending_count() == 0
    print("Queued task assigned after capacity release: PASS")

    empty_broker = DispatchBroker()
    assert empty_broker.assign_next(peers) is None
    assert empty_broker.reservation_count("peer-a") == 0

    print("\nAll Step 4 checks passed.")


if __name__ == "__main__":
    main()
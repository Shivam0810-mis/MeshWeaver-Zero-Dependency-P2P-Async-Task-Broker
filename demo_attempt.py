from meshweaver.broker.dispatcher import DispatchBroker
from meshweaver.broker.models import TaskStatus
from meshweaver.broker.peers import PeerInfo


def main():
    broker = DispatchBroker()

    task = broker.submit_task_once(
        "request-attempt-001",
        "add_numbers",
        10,
        20,
    )
    assert task.attempt_count == 0
    assert task.max_attempts == 3
    assert task.last_error is None
    print("Initial retry metadata: PASS")

    assert broker.assign_next([]) is None
    assert task.attempt_count == 0
    assert broker.pending_count() == 1
    print("No eligible peer leaves counter unchanged: PASS")

    peers = [
        PeerInfo(
            peer_id="peer-a",
            cpu_usage=10,
            running_tasks=0,
            max_tasks=1,
        )
    ]

    assignment = broker.assign_next(peers)

    assert assignment is not None
    assert assignment.task_id == task.task_id
    assert task.attempt_count == 1
    assert task.status == TaskStatus.ASSIGNING
    print("First assignment increments counter to 1: PASS")

    same_task = broker.submit_task_once(
        "request-attempt-001",
        "add_numbers",
        10,
        20,
    )

    assert same_task.task_id == task.task_id
    assert same_task.attempt_count == 1
    assert broker.pending_count() == 0
    print("Duplicate submission leaves counter unchanged: PASS")

    assert not broker.acknowledge(
        assignment.assignment_id,
        "wrong-peer",
    )
    assert task.status == TaskStatus.ASSIGNING
    assert task.attempt_count == 1

    assert broker.acknowledge(
        assignment.assignment_id,
        "peer-a",
    )
    assert broker.acknowledge(
        assignment.assignment_id,
        "peer-a",
    )
    assert task.status == TaskStatus.ASSIGNED
    assert task.attempt_count == 1
    print("ACK handling leaves counter unchanged: PASS")

    assert broker.mark_running(
        assignment.assignment_id,
        "peer-a",
    )
    assert broker.complete(
        assignment.assignment_id,
        "peer-a",
    )
    assert task.status == TaskStatus.COMPLETED
    assert task.attempt_count == 1
    print("Execution lifecycle leaves counter unchanged: PASS")

    assert broker.assign_next(peers) is None
    assert task.attempt_count == 1
    assert broker.reservation_count("peer-a") == 0
    print("Empty queue creates no assignment: PASS")

    print("\nAll Step 5A checks passed.")


if __name__ == "__main__":
    main()
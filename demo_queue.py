from meshweaver.broker.broker import Broker
from meshweaver.broker.models import TaskStatus


def main():
    broker = Broker()


    first = broker.submit_task_once(
        "request-101",
        "add_number",
        10,
        20,
    )


    duplicate = broker.submit_task_once(
        "request-101",
        "add_number",
        10,
        20,
    )

    second = broker.submit_task_once(
        "request-102",
        "multiply_number",
        3,
        4,
    )

    assert first.task_id == duplicate.task_id
    assert first.task_id != second.task_id
    assert broker.task_count() == 2
    assert broker.pending_count() == 2

    print("Duplicate return original task: PASS")
    print("Registered tasks:", broker.task_count())
    print("Pending tasks:", broker.pending_count())

    next_task = broker.dequeue_task()
    assert next_task is not None
    assert next_task.task_id == first.task_id
    assert next_task.status == TaskStatus.QUEUED
    print("First dequeued:", next_task.name)

    next_task = broker.dequeue_task()
    assert next_task is not None
    assert next_task.task_id == second.task_id
    print("Second dequeued:", next_task.name)

    assert broker.dequeue_task() is None
    assert broker.pending_count() == 0

    assert broker.get_task(first.task_id).task_id == first.task_id

    again = broker.submit_task_once(
        "request-101",
        "add_numbers",
        10,
        20,
    )
    assert again.task_id == first.task_id
    assert broker.pending_count() == 0

    try:
        broker.submit_task_once("   ", "add_numbers", 1, 2)
    except ValueError:
        pass
    else:
        raise AssertionError("Empty submission key was accepted.")

    print("Empty queue returns None: PASS")
    print("Deduplication after dequeue: PASS")
    print("Invalid key rejected: PASS")
    print("\nAll Step 2 checks passed.")


if __name__ == "__main__":
    main()


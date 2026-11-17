from meshweaver.broker.broker import Broker
from meshweaver.broker.models import TaskStatus


def main():
    broker = Broker()

    task = broker.submit_task(
        "add_numbers",
        10,
        20,
    )

    print("Task registered successfully")
    print("DEBUG RETURN:", task)
    print("Task ID:", task.task_id)
    print("Task name:", task.name)
    print("Arguments:", task.args)
    print("Keyword arguments:", task.kwargs)
    print("Status:", task.status.value)
    print("Created at:", task.created_at.isoformat())
    print("Total registered tasks:", broker.task_count())

    fetched_task = broker.get_task(task.task_id)

    assert fetched_task.task_id == task.task_id
    assert fetched_task.status == TaskStatus.QUEUED
    assert fetched_task.args == (10, 20)
    assert broker.task_count() == 1

    second_task = broker.submit_task("multiply_numbers", 3, 4)
    assert second_task.task_id != task.task_id
    assert broker.task_count() == 2

    try:
        broker.submit_task("   ")
    except ValueError:
        pass
    else:
        raise AssertionError("An empty task name should be rejected.")

    try:
        broker.get_task("unknown-task-id")
    except KeyError:
        pass
    else:
        raise AssertionError("An unknown task ID should raise KeyError.")

    print("\nAll registration checks passed.")


if __name__ == "__main__":
    main()
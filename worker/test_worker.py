
import pytest
import json
import asyncio
from worker.worker import Worker
from worker.result import WorkerResult


def add(a, b):
    return a + b


async def async_add(a, b):
    return a + b


# Worker execution tests

def test_normal_execution():
    worker = Worker()
    worker.register_handler("addition", add)

    assert worker.execute("addition", 10, 20) == 30


async def test_async_execution():
    worker = Worker()
    worker.register_handler("addition", async_add)

    result = await worker.execute_async("addition", 10, 20)

    assert result == 30


async def test_normal_function_async_execution():
    worker = Worker()
    worker.register_handler("addition", add)

    result = await worker.execute_async("addition", 5, 10)

    assert result == 15


async def test_unknown_handler():
    worker = Worker()

    with pytest.raises(ValueError):
        await worker.execute_async("unknown", 10, 20)


# Task validation tests

def test_empty_task_name():
    worker = Worker()

    with pytest.raises(ValueError):
        worker.register_handler("", add)


def test_whitespace_task_name():
    worker = Worker()

    with pytest.raises(ValueError):
        worker.register_handler("   ", add)


def test_invalid_task_name_type():
    worker = Worker()

    with pytest.raises(TypeError):
        worker.register_handler(123, add)


def test_non_callable_handler():
    worker = Worker()

    with pytest.raises(TypeError):
        worker.register_handler("addition", 123)


# Handler registration tests

def test_handler_registration():
    worker = Worker()

    worker.register_handler("addition", add)

    assert "addition" in worker.handlers
    assert worker.handlers["addition"] == add


def test_registered_handler_execution():
    worker = Worker()

    worker.register_handler("addition", add)

    result = worker.execute("addition", 10, 20)

    assert result == 30


# WorkerResult tests

def test_worker_result():
    output = WorkerResult("addition", 30)

    assert output.task_name == "addition"
    assert output.result == 30
    assert output.success is True
    assert output.error is None


def test_worker_result_to_dict():
    output = WorkerResult("addition", 30)

    expected = {
        "task_name": "addition",
        "success": True,
        "result": 30,
        "error": None
    }

    assert output.to_dict() == expected


def test_worker_result_to_json():
    output = WorkerResult("addition", 30)

    result = json.loads(output.to_json())

    assert result["task_name"] == "addition"
    assert result["success"] is True
    assert result["result"] == 30
    assert result["error"] is None


def test_successful_worker_result():
    output = WorkerResult("addition", result=30)

    assert output.success is True
    assert output.result == 30
    assert output.error is None


def test_failed_worker_result():
    output = WorkerResult(
        "division",
        error="Division by zero"
    )

    assert output.success is False
    assert output.result is None
    assert output.error == "Division by zero"
async def test_worker_dispatch():
    worker = Worker()
    worker.register_handler("addition", add)

    task = {
        "task_name": "addition",
        "args": [10, 20]
    }

    result = await worker.dispatch(task)

    assert result.task_name == "addition"
    assert result.success is True
    assert result.result == 30
    assert result.error is None
async def test_worker_receive_loop():
    worker = Worker()
    worker.register_handler("addition", add)

    runner = asyncio.create_task(worker.run())

    await worker.receive_task({
        "task_name": "addition",
        "args": [10, 20]
    })

    await worker.receive_task({
        "task_name": "addition",
        "args": [5, 15]
    })

    await worker.task_queue.join()

    assert len(worker.results) == 2
    assert worker.results[0].result == 30
    assert worker.results[1].result == 20

    runner.cancel()

    with pytest.raises(asyncio.CancelledError):
        await runner


@pytest.mark.asyncio
async def test_worker_respects_concurrency_limit():
    worker = Worker(max_concurrency=2)

    active_tasks = 0
    max_active_tasks = 0

    async def slow_task():
        nonlocal active_tasks, max_active_tasks

        active_tasks += 1
        max_active_tasks = max(max_active_tasks, active_tasks)

        await asyncio.sleep(0.1)

        active_tasks -= 1
        return "completed"

    worker.register_handler("slow", slow_task)

    runner = asyncio.create_task(worker.run())

    for _ in range(5):
        await worker.receive_task({
            "task_name": "slow",
            "args": []
        })

    await worker.task_queue.join()

    runner.cancel()

    with pytest.raises(asyncio.CancelledError):
        await runner

    assert len(worker.results) == 5
    assert max_active_tasks == 2

@pytest.mark.asyncio
async def test_worker_task_timeout():
    worker = Worker(task_timeout=1)

    async def slow_task():
        await asyncio.sleep(3)
        return "completed"

    worker.register_handler("slow_task", slow_task)

    result = await worker.dispatch({
        "task_name": "slow_task",
        "args": []
    })

    assert result.success is False
    assert result.result is None
    assert "timed out" in result.error


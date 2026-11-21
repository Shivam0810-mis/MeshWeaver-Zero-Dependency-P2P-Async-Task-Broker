
import pytest
import json

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

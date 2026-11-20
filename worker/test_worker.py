
import pytest
from worker.worker import Worker

def add(a, b):
    return a + b

async def async_add(a, b):
    return a + b

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

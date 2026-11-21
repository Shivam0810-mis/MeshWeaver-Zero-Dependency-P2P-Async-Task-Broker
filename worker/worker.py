
import asyncio


class Worker:
    def __init__(self):
        self.handlers = {}

    def validate_task_name(self, task_name):
        if not isinstance(task_name, str):
            raise TypeError("Task name must be a string")

        if not task_name.strip():
            raise ValueError("Task name cannot be empty")

    def register_handler(self, task_name, function):
        self.validate_task_name(task_name)

        if not callable(function):
            raise TypeError("Handler must be callable")

        self.handlers[task_name] = function

    def execute(self, task_name, *args):
        self.validate_task_name(task_name)

        if task_name not in self.handlers:
            raise ValueError("Handler not found")

        function = self.handlers[task_name]
        return function(*args)

    async def execute_async(self, task_name, *args):
        self.validate_task_name(task_name)

        if task_name not in self.handlers:
            raise ValueError("Handler not found")

        function = self.handlers[task_name]

        if asyncio.iscoroutinefunction(function):
            return await function(*args)

        return function(*args)
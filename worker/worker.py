import asyncio

class Worker:
    def __init__(self):
        self.handlers = {}

    def register_handler(self, task_name, function):
        self.handlers[task_name] = function

    def execute(self, task_name, *args):
        if task_name not in self.handlers:
            raise ValueError("Handler not found")

        function = self.handlers[task_name]
        return function(*args)

    async def execute_async(self, task_name, *args):
        if task_name not in self.handlers:
            raise ValueError("Handler not found")

        function = self.handlers[task_name]

        if asyncio.iscoroutinefunction(function):
            return await function(*args)

        return function(*args)
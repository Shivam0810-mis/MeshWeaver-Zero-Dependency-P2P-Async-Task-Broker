
import asyncio
from worker.result import WorkerResult


class Worker:

    def __init__(self, max_concurrency=2, task_timeout=5):
        self.handlers = {}
        self.task_queue = asyncio.Queue()
        self.results = []

        # Limit simultaneous task execution
        self.semaphore = asyncio.Semaphore(max_concurrency)

        # Maximum execution time allowed for each task
        self.task_timeout = task_timeout

    # Validate task name
    def validate_task_name(self, task_name):
        if not isinstance(task_name, str):
            raise TypeError("Task name must be a string")

        if not task_name.strip():
            raise ValueError("Task name cannot be empty")

    # Register a handler
    def register_handler(self, task_name, function):
        self.validate_task_name(task_name)

        if not callable(function):
            raise TypeError("Handler must be callable")

        self.handlers[task_name] = function

    # Execute a synchronous handler
    def execute(self, task_name, *args):
        self.validate_task_name(task_name)

        if task_name not in self.handlers:
            raise ValueError("Handler not found")

        function = self.handlers[task_name]

        return function(*args)

    # Execute synchronous or asynchronous handlers with timeout
    async def execute_async(self, task_name, *args):
        self.validate_task_name(task_name)

        if task_name not in self.handlers:
            raise ValueError("Handler not found")

        function = self.handlers[task_name]

        async def run_handler():
            if asyncio.iscoroutinefunction(function):
                return await function(*args)

            return function(*args)

        return await asyncio.wait_for(
            run_handler(),
            timeout=self.task_timeout
        )

    # Dispatch a task and create its result
    async def dispatch(self, task):
        task_name = task["task_name"]
        args = task.get("args", [])

        try:
            result = await self.execute_async(task_name, *args)

            return WorkerResult(
                task_name,
                result=result
            )

        except TimeoutError:
            return WorkerResult(
                task_name,
                error=f"Task execution timed out after {self.task_timeout} seconds"
            )

    # Receive a task and add it to the queue
    async def receive_task(self, task):
        await self.task_queue.put(task)

    # Process a task within the concurrency limit
    async def process_task(self, task):
        try:
            async with self.semaphore:
                result = await self.dispatch(task)
                self.results.append(result)

        finally:
            self.task_queue.task_done()

    # Continuously receive and process tasks
    async def run(self):
        while True:
            task = await self.task_queue.get()

            asyncio.create_task(
                self.process_task(task)
            )

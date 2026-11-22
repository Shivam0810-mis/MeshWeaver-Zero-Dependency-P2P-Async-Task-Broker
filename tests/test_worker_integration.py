
import unittest

from worker.worker import Worker

class TestWorkerIntegration(unittest.TestCase):

    def setUp(self):
        self.worker = Worker()

    def test_register_handler(self):
        self.worker.register_handler("addition", lambda a, b: a + b)

        self.assertIn("addition", self.worker.handlers)

    def test_execute_addition_task(self):
        self.worker.register_handler("addition", lambda a, b: a + b)

        result = self.worker.execute("addition", 10, 20)

        self.assertEqual(result, 30)

    def test_execute_task_with_multiple_arguments(self):
        self.worker.register_handler(
            "multiply",
            lambda a, b, c: a * b * c,
        )

        result = self.worker.execute("multiply", 2, 3, 4)

        self.assertEqual(result, 24)

    def test_execute_without_registered_handler(self):
        with self.assertRaises(ValueError):
            self.worker.execute("unknown_task", 10, 20)

    def test_handler_exception_is_propagated(self):
        def failing_handler():
            raise RuntimeError("Task execution failed")

        self.worker.register_handler("failing_task", failing_handler)

        with self.assertRaisesRegex(
            RuntimeError,
            "Task execution failed",
        ):
            self.worker.execute("failing_task")

if __name__ == "__main__":
    unittest.main()
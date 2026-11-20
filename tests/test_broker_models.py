import json
import types
import unittest

import meshweaver.broker.scheduler as scheduler_module
from meshweaver.broker import (
    ALLOWED_TRANSITIONS,
    TERMINAL_STATUSES,
    BrokerProtocol,
    InvalidTaskTransitionError,
    PeerSelectorProtocol,
    Scheduler,
    Task,
    TaskNotFoundError,
    TaskQueueProtocol,
    TaskStatus,
    TaskTransportProtocol,
    TaskValidationError,
)
from meshweaver.core.errors import MeshWeaverError, TaskError


class TestBrokerModelsAndInterfaces(unittest.TestCase):
    def test_broker_errors_inherit_from_shared_task_error(self):
        self.assertTrue(issubclass(TaskValidationError, TaskError))
        self.assertTrue(issubclass(TaskValidationError, ValueError))
        self.assertTrue(issubclass(TaskNotFoundError, TaskError))
        self.assertTrue(issubclass(TaskNotFoundError, KeyError))
        self.assertTrue(issubclass(InvalidTaskTransitionError, TaskError))
        self.assertTrue(issubclass(InvalidTaskTransitionError, MeshWeaverError))

    def test_task_defaults_and_json_snapshot(self):
        task = Task(name="  add_numbers  ", args=(10, 20), kwargs={"scale": 2})

        self.assertEqual(task.name, "add_numbers")
        self.assertEqual(task.args, (10, 20))
        self.assertEqual(task.kwargs, {"scale": 2})
        self.assertEqual(task.status, TaskStatus.QUEUED)
        self.assertEqual(task.attempt_count, 0)
        self.assertEqual(task.max_attempts, 3)
        self.assertEqual(task.attempts_remaining, 3)
        self.assertTrue(task.can_retry)
        self.assertFalse(task.is_terminal)
        self.assertIsNone(task.last_error)

        snapshot = task.to_dict()
        encoded = json.dumps(snapshot)
        decoded = json.loads(encoded)
        self.assertEqual(decoded["task_id"], task.task_id)
        self.assertEqual(decoded["name"], "add_numbers")
        self.assertEqual(decoded["args"], [10, 20])
        self.assertEqual(decoded["status"], "QUEUED")
        self.assertEqual(decoded["max_attempts"], 3)

    def test_task_validation_rejects_bad_inputs(self):
        with self.assertRaises(TaskValidationError):
            Task(name="   ")

        with self.assertRaises(TaskValidationError):
            Task(name="add", max_attempts=0)

        with self.assertRaises(TaskValidationError):
            Task(name="add", max_attempts=True)

        with self.assertRaises(TaskValidationError):
            Task(name="add", args="not-a-tuple")

        with self.assertRaises(TaskValidationError):
            Task(name="add", kwargs=["not", "a", "dict"])

    def test_valid_lifecycle_transitions_and_error_recording(self):
        task = Task(name="compute")
        self.assertTrue(task.can_transition_to(TaskStatus.ASSIGNING))
        self.assertFalse(task.can_transition_to(TaskStatus.COMPLETED))

        task.transition_to(TaskStatus.ASSIGNING)
        task.transition_to(TaskStatus.ASSIGNED)
        task.transition_to(TaskStatus.RUNNING)
        task.transition_to(TaskStatus.RETRY_WAIT, error="worker timeout")
        self.assertEqual(task.last_error, "worker timeout")

        task.transition_to(TaskStatus.QUEUED)
        task.transition_to(TaskStatus.ASSIGNING)
        task.transition_to(TaskStatus.ASSIGNED)
        task.transition_to(TaskStatus.COMPLETED)
        self.assertTrue(task.is_terminal)
        self.assertFalse(task.can_retry)

    def test_terminal_states_block_further_transitions(self):
        for terminal in TERMINAL_STATUSES:
            self.assertEqual(ALLOWED_TRANSITIONS[terminal], frozenset())

        task = Task(name="compute")
        task.transition_to(TaskStatus.FAILED, error="fatal error")
        self.assertTrue(task.is_terminal)

        with self.assertRaises(InvalidTaskTransitionError):
            task.transition_to(TaskStatus.QUEUED)

    def test_invalid_transition_raises_error(self):
        task = Task(name="compute")
        with self.assertRaises(InvalidTaskTransitionError):
            task.transition_to(TaskStatus.RUNNING)

        with self.assertRaises(InvalidTaskTransitionError):
            task.transition_to("NOT_A_STATUS")  # type: ignore[arg-type]

    def test_protocols_exported(self):
        for proto in (
            BrokerProtocol,
            TaskQueueProtocol,
            PeerSelectorProtocol,
            TaskTransportProtocol,
        ):
            self.assertTrue(hasattr(proto, "__protocol_attrs__") or hasattr(proto, "_is_protocol"))

    def test_scheduler_submodule_not_shadowed_by_package_export(self):
        self.assertIsInstance(scheduler_module, types.ModuleType)
        self.assertIs(Scheduler, scheduler_module.scheduler)


if __name__ == "__main__":
    unittest.main()

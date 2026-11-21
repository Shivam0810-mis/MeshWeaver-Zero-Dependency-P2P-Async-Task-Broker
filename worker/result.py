import json

class WorkerResult:
    def __init__(self, task_name, result=None, error=None):
        self.task_name = task_name
        self.result = result
        self.error = error
        self.success = error is None

    def to_dict(self):
        return {
            "task_name": self.task_name,
            "success": self.success,
            "result": self.result,
            "error": self.error
        }

    def to_json(self):
        return json.dumps(self.to_dict())
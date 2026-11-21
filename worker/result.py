import json
class WorkerResult:
    def __init__(self, task_name, result):
        self.task_name = task_name
        self.result = result
    def to_dict(self):
        return{
            "task_name":self.task_name,
            "result":self.result
        }
    def to_json(self):
        return json.dumps(self.to_dict())
    
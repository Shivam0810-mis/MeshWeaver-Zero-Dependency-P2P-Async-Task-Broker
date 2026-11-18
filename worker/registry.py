class HandlerRegistry:
    def __init__(self):
        self.handlers = {}
    def register(self,task_name,function):
        self.handlers[task_name]=function
    def get_handler(self,task_name):
        if task_name not in self.handlers:
            raise ValueError("Handler not found")
        return self.handlers[task_name]
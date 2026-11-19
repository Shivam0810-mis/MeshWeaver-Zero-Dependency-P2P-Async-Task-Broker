from worker.worker import Worker
from worker.registry import HandlerRegistry

def add(a,b):
    return a+b
worker=Worker()
worker.register_handler("addition",add)
print(worker.handlers)
result=worker.execute("addition",10,20)
print(result)
registry = HandlerRegistry()

registry.register("addition", add)

function = registry.get_handler("addition")

print(function(10, 20))
try:
    registry.get_handler("multiplication")
except ValueError as e:
    print(e)
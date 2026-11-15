from worker.worker import Worker
def add(a,b):
    return a+b
worker=Worker()
worker.register_handler("addition",add)
print(worker.handlers)
result=worker.execute("addition",10,20)
print(result)
worker.execute("multiplication", 5, 2)
from celery import Celery
import os
import time

app = Celery(__name__)
#app.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "amqp://guest:guest@localhost:5672//")
#app.conf.result_backend = os.environ.get("CELERY_RESULT_BACKEND", "rpc://guest:guest@localhost:5672//")
app.conf.broker_url = "amqp://tnorabbit:tno123tno@rabbit:5672/tno"
app.conf.result_backend = "rpc://tnorabbit:tno123tno@rabbit:5672/tno"

@app.task(name="add")
def add(x, y):
    return x + y

@app.task(name="create_task")
def create_task(task_type):
    time.sleep(int(task_type) * 10)
    return True
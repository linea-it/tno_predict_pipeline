from tno_celery.celery import app
from service import orbit_trace_job_queue

@app.task
def add(x, y):
    return x + y

@app.task
def mul(x, y):
    return x * y


@app.task
def xsum(numbers):
    return sum(numbers)

@app.task
def orbit_trace_queue():
    orbit_trace_job_queue()
from fastapi import FastAPI
from fastapi import Body, FastAPI, Form, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from worker import create_task

app = FastAPI()

@app.get("/")
def hello_root():
    return {"message": "Hello World"}



@app.post("/tasks", status_code=201)
def run_task(payload = Body(...)):
    task_type = payload["type"]
    task = create_task.delay(int(task_type))
    return JSONResponse({"task_id": task.id})

# https://testdriven.io/blog/fastapi-and-celery/
# https://github.com/testdrivenio/fastapi-celery/blob/master/docker-compose.yml
# https://www.toptal.com/python/orchestrating-celery-python-background-jobs
# https://medium.com/cuddle-ai/async-architecture-with-fastapi-celery-and-rabbitmq-c7d029030377
# https://derlin.github.io/introduction-to-fastapi-and-celery/03-celery/


# Para testar: 
# curl http://backend:8000/tasks -H "Content-Type: application/json" --data '{"type": 0}'

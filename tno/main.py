from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def hello_root():
    return {"message": "Hello World"}


# https://testdriven.io/blog/fastapi-and-celery/
# https://github.com/testdrivenio/fastapi-celery/blob/master/docker-compose.yml
# https://www.toptal.com/python/orchestrating-celery-python-background-jobs
# https://medium.com/cuddle-ai/async-architecture-with-fastapi-celery-and-rabbitmq-c7d029030377
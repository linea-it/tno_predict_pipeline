from celery import Celery
from celery.schedules import crontab

broker_url = 'amqp://tno:tnorabbit123@srvnode04:56722//'
# 'amqp://myuser:mypassword@localhost:5672/myvhost'
result_backend = 'db+sqlite:///results.db'
# include=['tasks']

app = Celery('tasks', backend=result_backend, broker=broker_url)

# Optional configuration, see the application user guide.
app.conf.update(
    result_expires=3600,
    timezone = 'UTC'
)

app.conf.beat_schedule = {
    'add-every-30-seconds': {
        'task': 'tasks.add',
        'schedule': 30.0,
        'args': (16, 16)
    },
    #  "schedule": crontab(minute="*/1"),
}
# app.conf.timezone = 'UTC'

if __name__ == '__main__':
    app.start()
from celery import Celery
# from celery.schedules import crontab

broker_url = 'amqp://tno:tnorabbit123@srvnode04:56723/tno_vhost'

result_backend = 'db+sqlite:///results.db'
include=['tno_celery.tasks']

app = Celery('tno', broker=broker_url, backend=result_backend, include=include)
# app.conf.task_default_queue = 'default'

# Optional configuration, see the application user guide.
app.conf.update(
    result_expires=3600,
    timezone = 'UTC'
)

app.conf.beat_schedule = {
    # Verifica a tabela de jobs de ocultação a cada 30 segundos a procura de jobs a serem executados.
    # 'orbit-check-to-run': {
    #     'task': 'tno_celery.tasks.orbit_trace_queue',
    #     'schedule': 30.0,
    #     # 'options': {'queue' : 'single'}
    # },
    # Verifica a tabela de jobs de predição a cada 30 segundos a procura de jobs a serem executados.
    'predict-check-to-run': {
        'task': 'tno_celery.tasks.predict_occultation_queue',
        'schedule': 30.0,
    },
    #'predict-check-running': {
    #    'task': 'tno_celery.tasks.predict_occultation_running',
    #    'schedule': 30.0,
    #},            
}

if __name__ == '__main__':
    app.start()

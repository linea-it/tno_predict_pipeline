from predict_occultation import (
    rerun_job,
)

rerun_job(132)


# rerun_job(83)

# main('/lustre/t1/tmp/tno/orbit_trace/17-cdcb626a')
# ingest_job_results('/lustre/t1/tmp/tno/orbit_trace/16-6930783e', 16)

# Como iniciar o Celery
# celery -A tno_celery worker -l INFO
# celery -A tno_celery beat -l INFO

# celery -A tno_celery worker -Q single -c 1 --detach -l INFO --pidfile="/lustre/t1/tmp/tno/pipelines/tmp/%n.pid" --logfile="/lustre/t1/tmp/tno/pipelines/tmp/%n%I.log"
# celery -A tno_celery worker -Q default --detach -l INFO --pidfile="/lustre/t1/tmp/tno/pipelines/tmp/%n.pid" --logfile="/lustre/t1/tmp/tno/pipelines/tmp/%n%I.log"
# celery -A tno_celery beat --detach -l INFO --pidfile="/lustre/t1/tmp/tno/pipelines/tmp/celeryd.pid" --logfile="/lustre/t1/tmp/tno/pipelines/tmp/celeryd.log"


# Listar todos processos do meu usuario
# ps -f -U 15161

# Comando para listar os processos do celery worker
# ps aux|grep 'celery worker'
# ps aux|grep 'celery beat'

# Comando para matar todos os processos do celery worker
# ps auxww | grep 'celery worker' | awk '{print $2}' | xargs kill -9
# ps auxww | grep 'celery beat' | awk '{print $2}' | xargs kill -9

# run_predict_job(57)
# rerun_job(57)
# check_tasks(56)
# predict_job_queue()

# from pathlib import Path

# rerun_job(64)
# from time import sleep
# flag = False
# while flag != True:
#     flag = check_tasks(64)
#     sleep(30)

# fp = Path('/lustre/t1/tmp/tno/predict_occultation/57')
# count_results_ingested = ingest_job_results(fp, 57)

# check_tasks(61)

# from predict_occultation import get_job_running

# running = get_job_running()
# print(running)

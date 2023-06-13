# import os
# original_path = os.getcwd()
# os.environ["EXECUTION_PATH"] = original_path

from pathlib import Path
import uuid
from library import get_configs, write_job_file
from dao import OrbitTraceJobDao, OrbitTraceJobResultDao, PredictOccultationJobDao
# from orbit_trace import orbit_trace_job_to_run, orbit_trace_has_job_running, orbit_trace_make_job_json_file
from datetime import timezone
import shutil
import subprocess
from datetime import datetime, timezone
from predict_occultation import predict_job_queue, run_job as run_predict_job
from orbit_trace import run_job, ingest_job_results, main

# run_predict_job(43)

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




# from tno_celery.tasks import add
# result = add.delay(4, 4)
# # result.ready()
# print(result.ready())
# a = result.get(timeout=1)

# print("Antes de enviar a task")
# from tno_celery.tasks import orbit_trace_queue, orbit_trace_run
# print("task enviada")
# result = orbit_trace_queue.delay()
# print("antes task ready")
# result.ready()
# print("task Ready (%s)" % result.ready())
# a = result.get()
# print(a)

# def job_to_run():
#     """Retorna o job com status=1 Idle mas antigo.

#     Returns:
#         job: Orbit Trace Job model
#     """
#     dao = PredictOccultationJobDao()
#     job = dao.get_job_by_status(1)

#     return job

# def has_job_running() -> bool:
#     """Verifica se há algum job com status = 2 Running.

#     Returns:
#         bool: True caso haja algum job sendo executado.
#     """
#     dao = PredictOccultationJobDao()
#     job = dao.get_job_by_status(2)

#     if job is not None:
#         return True
#     else:
#         return False

# def make_job_json_file(job, path):

#     job_data = dict({
#         "id": job.get('id'),
#         "status": "Submited",
#         "submit_time": job.get('submit_time').astimezone(timezone.utc).isoformat(),
#         "estimated_execution_time": str(job.get('estimated_execution_time')),
#         "path": str(path),
#         "filter_type": job.get('filter_type'),
#         "filter_value": job.get('filter_value'),
#         "predict_start_date": job.get("predict_start_date").isoformat(),
#         "predict_end_date": job.get("predict_end_date").isoformat(),
#         "predict_step": job.get("predict_step", 600),        
#         "debug": bool(job.get('debug', False)),
#         "error": None,
#         "traceback": None,
#         # Parsl init block não está sendo utilizado no pipeline
#         # "parsl_init_block": int(job.get('parsl_init_block', 600)),
#         # TODO: Adicionar parametro para catalog 
#         # "catalog_id": job.get('catalog_id')
#         # TODO: Estes parametros devem ser gerados pelo pipeline lendo do config.
#         # TODO: Bsp e leap second deve fazer a query e verificar o arquivo ou fazer o download.
#         "bsp_planetary": {
#             "name": "de440",
#             "filename": "de440.bsp",
#             "absolute_path": "/lustre/t1/tmp/tno/bsp_planetary/de440.bsp",
#         },
#         "leap_seconds": {
#             "name": "naif0012",
#             "filename": "naif0012.tls",
#             "absolute_path": "/lustre/t1/tmp/tno/leap_seconds/naif0012.tls",
#         },           
#         # "force_refresh_inputs": False,
#         # "inputs_days_to_expire": 5,        
#     })

#     write_job_file(path, job_data)

# def run_job(jobid: int):

#     dao = PredictOccultationJobDao()

#     # TODO: ONLY DEVELOPMENT
#     # dao.development_reset_job(jobid)

#     job = dao.get_job_by_id(jobid)

#     config = get_configs()
#     orbit_trace_root = config["DEFAULT"].get("PredictOccultationJobPath")

#     # Cria um diretório para o job
#     # TODO: ONLY DEVELOPMENT
#     folder_name = f"teste_{job['id']}"
#     # folder_name = f"teste_{job['id']}-{str(uuid.uuid4())[:8]}"    
#     job_path = Path(orbit_trace_root).joinpath(folder_name)
#     if job_path.exists():
#         shutil.rmtree(job_path)
#     job_path.mkdir(parents=True, exist_ok=False)

#     # Escreve o arquivo job.json
#     make_job_json_file(job, job_path)

#     # # Executa o job usando subproccess.
#     # env_file = Path(os.environ['EXECUTION_PATH']).joinpath('env.sh')
#     # proc = subprocess.Popen(
#     #     # f"source /lustre/t1/tmp/tno/pipelines/env.sh; python orbit_trace.py {job_path}",
#     #     f"source {env_file}; python orbit_trace.py {job_path}",
#     #     stdout=subprocess.PIPE,
#     #     stderr=subprocess.PIPE,
#     #     shell=True,
#     #     text=True
#     # )

#     # import time
#     # while proc.poll() is None:
#     #     print("Shell command is still running...")
#     #     time.sleep(1)

#     # # When arriving here, the shell command has finished.
#     # # Check the exit code of the shell command:
#     # print(proc.poll())
#     # # 0, means the shell command finshed successfully.

#     # # Check the output and error of the shell command:
#     # output, error = proc.communicate()
#     # print(output)
#     # print(error)

# def job_queue():

#     # Verifica se ha algum job sendo executado.
#     if has_job_running():
#         # print("Já existe um job em execução.")
#         return

#     # Verifica o proximo job com status Idle
#     to_run = job_to_run()
#     if not to_run:
#         # print("Nenhum job para executar.")
#         return

#     # Inicia o job.
#     # print("Deveria executar o job com ID: %s" % to_run.get("id")) 
#     run_job(to_run.get("id"))

# job_queue()

# TODO: Já escreve o arquivo job json 
# - Falta alterar o pipeline predict para executar com chamada de função.
# - Falta alterar o metodo de update do job.

# from predict_occultation import main, ingest_job_results
# job_id = 16
# job_path = f"/lustre/t1/tmp/tno/predict_occultation/teste_{job_id}"
# run_job(job_id)
# main(job_path)
# ingest_job_results(job_path, job_id)

# run_predict_job(44)
predict_job_queue()
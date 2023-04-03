# import os
# original_path = os.getcwd()
# os.environ["EXECUTION_PATH"] = original_path

from pathlib import Path
import uuid
from library import get_configs, write_job_file
from dao import OrbitTraceJobDao, OrbitTraceJobResultDao
from orbit_trace import orbit_trace_job_to_run, orbit_trace_has_job_running, orbit_trace_make_job_json_file
from datetime import timezone
import shutil
import subprocess

# def orbit_trace_job_to_run():
#     """Retorna o job com status=1 Idle mas antigo.

#     Returns:
#         _type_: _description_
#     """
#     # from dao import OrbitTraceJobDao

#     otjdao = OrbitTraceJobDao()
#     job = otjdao.get_job_by_status(1)

#     return job


# def orbit_trace_has_job_running() -> bool:
#     """Verifica se há algum job com status = 2 Running.

#     Returns:
#         bool: True caso haja algum job sendo executado.
#     """
#     # from dao import OrbitTraceJobDao

#     otjdao = OrbitTraceJobDao()
#     job = otjdao.get_job_by_status(2)

#     if job is not None:
#         return True
#     else:
#         return False


# def orbit_trace_make_job_json_file(job, path):

#     job_data = dict({
#         "id": job.get('id'),
#         "status": "Submited",
#         "submit_time": job.get('submit_time').astimezone(timezone.utc).isoformat(),
#         "estimated_execution_time": str(job.get('estimated_execution_time')),
#         "path": str(path),
#         "match_radius": job.get('match_radius'),
#         "filter_type": job.get('filter_type'),
#         "filter_value": job.get('filter_value'),
#         "bsp_days_to_expire": job.get('bps_days_to_expire'),
#         "parsl_init_blocks": job.get('parsl_init_blocks'),        
#         "debug": bool(job.get('debug')),
#         "traceback": None,
#         "error": None,
#         "time_profile": [],     
#         # TODO: Estes parametros devem ser gerados pelo pipeline lendo do config.
#         # TODO: Bsp e leap second deve fazer a query e verificar o arquivo ou fazer o download.
#         "period": ["2012-11-01", "2019-02-01"],
#         "observatory_location": [289.193583333, -30.16958333, 2202.7],
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
#     })

#     write_job_file(path, job_data)


def orbit_trace_run_job(jobid: int):
    # TODO: ONLY DEVELOPMENT
    otjdao = OrbitTraceJobDao()    
    otjdao.development_reset_job(jobid)

    job = otjdao.get_job_by_id(jobid)

    config = get_configs()
    orbit_trace_root = config["DEFAULT"].get("OrbitTraceJobPath")

    # Cria um diretório para o job
    # folder_name = f"teste_{job['id']}-{str(uuid.uuid4())[:8]}"
    folder_name = f"teste_{job['id']}"
    job_path = Path(orbit_trace_root).joinpath(folder_name)
    if job_path.exists():
        shutil.rmtree(job_path)
    job_path.mkdir(parents=True, exist_ok=False)

    # Escreve o arquivo job.json
    orbit_trace_make_job_json_file(job, job_path)


    # teste(job_path=job_path)

    # Executa o job.
    proc = subprocess.Popen(
        # Let it ping more times to run longer.
        # f"source env.sh; python orbit_trace.py {job_path}",
        f"source /lustre/t1/tmp/tno/pipelines/env.sh; python orbit_trace.py {job_path}",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=True,
        text=True
    )

    # import time
    # while proc.poll() is None:
    #     print("Shell command is still running...")
    #     time.sleep(1)

    # # When arriving here, the shell command has finished.
    # # Check the exit code of the shell command:
    # print(proc.poll())
    # # 0, means the shell command finshed successfully.

    # # Check the output and error of the shell command:
    # output, error = proc.communicate()
    # print(output)
    # print(error)

def orbit_trace_job_queue():

    # Verifica se ha algum job sendo executado.
    if orbit_trace_has_job_running():
        print("Já existe um job em execução.")
        return

    # Verifica o proximo job com status Idle
    job_to_run = orbit_trace_job_to_run()
    if not job_to_run:
        return

    # Inicia o job.
    print("Deveria executar o job com ID: %s" % job_to_run.get("id")) 
    # orbit_trace_run_job(job_to_run.get("id"))
    # # TODO: Remover Hardcoded.
    # orbit_trace_run_job(3)

# orbit_trace_job_queue()

# path = '/home/mehdi/PycharmProjects'
# import sys
# sys.path.append(path)

from celery_app.tasks import add
result = add.delay(4, 4)
result.ready()
a = result.get(timeout=1)

print(a)
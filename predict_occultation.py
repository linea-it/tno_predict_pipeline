# -*- coding: utf-8 -*-

import argparse
import os
from datetime import datetime, timezone
from sqlalchemy import engine
import time
import traceback
import pathlib
import json
import humanize
import pathlib
import pandas as pd
import configparser
from condor import Condor
import pandas as pd
import random
from asteroid import Asteroid
import sys
from dao import PredictOccultationJobDao, PredictOccultationJobResultDao, PredictOccultationJobStatusDao
from io import StringIO
from library import get_configs
import shutil
import subprocess
import uuid

from library import (
    get_logger,
    read_inputs,
    write_job_file,
    retrieve_asteroids,
    submit_job,
)

def job_to_run():
    """Retorna o job com status=1 Idle mas antigo.

    Returns:
        job: Orbit Trace Job model
    """
    dao = PredictOccultationJobDao()
    job = dao.get_job_by_status(1)

    return job

def has_job_running() -> bool:
    """Verifica se há algum job com status = 2 Running.

    Returns:
        bool: True caso haja algum job sendo executado.
    """
    dao = PredictOccultationJobDao()
    job = dao.get_job_by_status(2)

    if job is not None:
        return True
    else:
        return False

def get_job_running():
    """Verifica se há algum job com status = 2 Running.

    Returns:
        Id: Job caso 
    """
    dao = PredictOccultationJobDao()
    job = dao.get_job_by_status(2)

    if job is not None:
        return job.get('id')


def make_job_json_file(job, path):

    job_data = dict({
        "id": job.get('id'),
        "status": "Idle",
        "submit_time": job.get('submit_time').astimezone(timezone.utc).isoformat(),
        "estimated_execution_time": str(job.get('estimated_execution_time')),
        "path": str(path),
        "filter_type": job.get('filter_type'),
        "filter_value": job.get('filter_value'),
        "predict_start_date": job.get("predict_start_date").isoformat(),
        "predict_end_date": job.get("predict_end_date").isoformat(),
        "predict_step": job.get("predict_step", 600),        
        "debug": bool(job.get('debug', False)),
        "error": None,
        "traceback": None,
        # Parsl init block não está sendo utilizado no pipeline
        # "parsl_init_block": int(job.get('parsl_init_block', 600)),
        # TODO: Adicionar parametro para catalog 
        # "catalog_id": job.get('catalog_id')
        # TODO: Estes parametros devem ser gerados pelo pipeline lendo do config.
        # TODO: Bsp e leap second deve fazer a query e verificar o arquivo ou fazer o download.
        "bsp_planetary": {
            "name": "de440",
            "filename": "de440.bsp",
            "absolute_path": "/lustre/t1/tmp/tno/bsp_planetary/de440.bsp",
        },
        "leap_seconds": {
            "name": "naif0012",
            "filename": "naif0012.tls",
            "absolute_path": "/lustre/t1/tmp/tno/leap_seconds/naif0012.tls",
        },                  
        # "force_refresh_inputs": False,
        # "inputs_days_to_expire": 5,        
    })

    write_job_file(path, job_data)
    update_job(job_data)

def rerun_job(jobid: int): 
    daojob = PredictOccultationJobDao()

    # Faz um update na tabela de job zerando os campos.
    daojob.development_reset_job(jobid)

    daoresult = PredictOccultationJobResultDao()
    daoresult.delete_by_job_id(jobid)

    daostatus = PredictOccultationJobStatusDao()
    daostatus.delete_by_job_id(jobid)

    run_job(jobid)

def run_job(jobid: int):
    print("run job: %s" % jobid)

    dao = PredictOccultationJobDao()
   
    job = dao.get_job_by_id(jobid)

    config = get_configs()
    orbit_trace_root = config["DEFAULT"].get("PredictOccultationJobPath")

    # Cria um diretório para o job
    # TODO: ONLY DEVELOPMENT
    # folder_name = f"teste_{job['id']}"
    # folder_name = f"{job['id']}-{str(uuid.uuid4())[:8]}"    
    folder_name = f"{job['id']}"
    job_path = pathlib.Path(orbit_trace_root).joinpath(folder_name)
    if job_path.exists():
        shutil.rmtree(job_path)
    job_path.mkdir(parents=True, exist_ok=False)

    print("Create Job folder")

    # Escreve o arquivo job.json
    make_job_json_file(job, job_path)
    print("Make job json")

    return submit_tasks(jobid)

    # # Executa o job usando subproccess.
    # env_file = pathlib.Path(os.environ['EXECUTION_PATH']).joinpath('env.sh')
    # proc = subprocess.Popen(
    #     # f"source /lustre/t1/tmp/tno/pipelines/env.sh; python orbit_trace.py {job_path}",
    #     f"source {env_file}; python predict_occultation.py {job_path}",
    #     stdout=subprocess.PIPE,
    #     stderr=subprocess.PIPE,
    #     shell=True,
    #     text=True
    # )
    
    # [DESENVOLVIMENTO] descomentar este bloco para que o função execute. 
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


def predict_job_queue():

    # Verifica se ha algum job sendo executado.
    if has_job_running():
        # print("Já existe um job em execução.")
        return

    # Verifica o proximo job com status Idle
    to_run = job_to_run()
    if not to_run:
        # print("Nenhum job para executar.")
        return

    # Inicia o job.
    # print("Deveria executar o job com ID: %s" % to_run.get("id")) 
    run_job(to_run.get("id"))

def update_job(job) -> None:
    dao = PredictOccultationJobDao()
    dao.update_job(job)

    write_job_file(job.get('path'), job)

def ingest_job_results(job_path, job_id):
    dao = PredictOccultationJobResultDao()
    dao.delete_by_job_id(job_id)

    filepath = pathlib.Path(job_path, "job_consolidated.csv")

    df = pd.read_csv(
        filepath, 
        delimiter=";",
        usecols=[
        "ast_id", "name", "number", "base_dynclass", "dynclass", 
        "des_obs", "obs_source", "orb_ele_source", "pre_occ_count", "ing_occ_count",
        "messages", "exec_time", "status", 
        "des_obs_start", "des_obs_finish", "des_obs_exec_time",
        "bsp_jpl_start", "bsp_jpl_finish", "bsp_jpl_dw_time",
        "obs_start", "obs_finish", "obs_dw_time",
        "orb_ele_start", "orb_ele_finish", "orb_ele_dw_time",
        "ref_orb_start", "ref_orb_finish", "ref_orb_exec_time",
        "pre_occ_start", "pre_occ_finish", "pre_occ_exec_time",
        "ing_occ_start", "ing_occ_finish", "ing_occ_exec_time"
        ]
        )
    df['job_id'] = int(job_id)
    df = df.rename(
        columns={
            "ast_id": "asteroid_id",
            "pre_occ_count": "occultations"
        })
    
    df["des_obs"].fillna(0, inplace=True)
    df["occultations"].fillna(0, inplace=True)
    df["ing_occ_count"].fillna(0, inplace=True)

    # TODO: Remover
    df["status"].fillna(1, inplace=True)

    df = df.astype({
        "des_obs": 'int32',
        "occultations": 'int32',
        "ing_occ_count": 'int32',
        "asteroid_id": 'int32',
        "job_id": 'int32',
        "status": 'int32',
        })
    
    df = df.reindex(
        columns=[
        "name", "number", "base_dynclass", "dynclass", 
        "status", "des_obs", "obs_source", "orb_ele_source", 
        "occultations", "ing_occ_count", "exec_time", "messages", 
        "asteroid_id", "job_id",
        "des_obs_start", "des_obs_finish", "des_obs_exec_time",
        "bsp_jpl_start", "bsp_jpl_finish", "bsp_jpl_dw_time",
        "obs_start", "obs_finish", "obs_dw_time",
        "orb_ele_start", "orb_ele_finish", "orb_ele_dw_time",
        "ref_orb_start", "ref_orb_finish", "ref_orb_exec_time",
        "pre_occ_start", "pre_occ_finish", "pre_occ_exec_time",
        "ing_occ_start", "ing_occ_finish", "ing_occ_exec_time"
        ])
                
    data = StringIO()
    df.to_csv(
        data, sep="|", header=True, index=False,
    )
    data.seek(0)

    rowcount = dao.import_predict_occultation_results(data)

    return rowcount

def get_configs():
    # Carrega as variaveis de configuração do arquivo config.ini
    config = configparser.ConfigParser()
    config.read("config.ini")
    return config

def read_job_json_by_id(jobid):
    dao = PredictOccultationJobDao()
    job_db = dao.get_job_by_id(jobid)
    # Read Inputs from job.json
    return read_inputs(job_db['path'], "job.json")

def write_htc_job_file(path: pathlib.Path, data: dict):
    lockf = path.joinpath("lock")
    try:
        with open(lockf, mode='w'): pass

        fp = path.joinpath(f"{data.get('ClusterId')}.json")
        with open(fp, "w") as json_file:
            json.dump(data, json_file)

        return fp

    finally:
        lockf.unlink()

def read_htc_job_file(path: pathlib.Path):
    with open(path) as json_file:
        data = json.load(json_file)
        return data


def update_progress_status(
        job_id: int, 
        step: int,
        t0: datetime,
        status: int = None,
        count: int = 0,
        current: int = 0,
        success: int = 0,
        failures: int = 0):
    
    t1 = datetime.now(tz=timezone.utc)
    tdelta = t1 - t0
    tdelta = tdelta.total_seconds()
    average_time = tdelta / count
    time_estimate = tdelta * (count - current)

    tasks = ['Data acquisition and preparation', 'Refine Orbit and Predict Occultation']
    task = tasks[step -1]

    dao = PredictOccultationJobStatusDao()
    dao.update_or_insert(job_id, step, task, status, count, current, average_time, time_estimate, success, failures)

def setup_job_status(jobid, count_asteroids):
    update_progress_status(
        jobid, 
        step=1,
        status=2, 
        count=count_asteroids,
        current = 0,
        success = 0,
        failures= 0,
        t0=datetime.now(tz=timezone.utc))
        
    update_progress_status(
        jobid, 
        step=2,
        status=2, 
        count=count_asteroids,
        current = 0,
        success = 0,
        failures= 0,
        t0=datetime.now(tz=timezone.utc))        

def submit_tasks(jobid: int):
    print("submit_tasks")
    config = get_configs()

    # Read Inputs from job.json
    job = read_job_json_by_id(jobid)

    # Paths de execução
    original_path = os.getcwd()
    current_path = pathlib.Path(job.get('path'))

    DEBUG = job.get("debug", False)

    # Create a Log file
    logname = "submit_tasks"
    log = get_logger(current_path, f"{logname}.log", DEBUG)

    current_idx = 1
    success = 0
    failures = 0 
    hb_t0 = datetime.now(tz=timezone.utc)

    try:
        # Altera o path de execução
        # A raiz agora é o path passado como parametro.
        os.chdir(current_path)

        # Start Running Time
        t0 = datetime.now(tz=timezone.utc)

        log.info("--------------< Predict Occultation Pipeline >--------------")
        log.info("Job ID: [%s]" % jobid)
        log.info("Current Path: [%s]" % current_path)
        log.info("DEBUG: [%s]" % DEBUG)

        job.update(
            {
                "status": "Running",
                "start": t0.isoformat(),
                "end": None,
                "exec_time": 0,
                "count_asteroids": 0,
                "count_success": 0,
                "count_failures": 0,
                "ast_with_occ": 0,
                "occultations": 0,
                "time_profile": [],
                "submited_all_jobs": False,
                "condor_job_submited": 0,
                "condor_job_completed": 0,
                "condor_job_removed": 0,
                "check_tasks": None
            }
        )

        log.info("Update Job status to running.")
        update_job(job)

        # =========================== Parameters ===========================

        # ASTEROID_PATH: Diretório onde serão armazenados todos os arquivos referentes
        # aos Asteroids, dentro deste diretório serão criados diretorios para cada
        # Asteroid contendo seus arquivos de inputs e outputs.
        # Atenção: Precisar permitir uma quantidade grande de acessos de leitura e escrita simultaneas.
        # ASTEROID_PATH = config["DEFAULT"].get("AsteroidPath")
        # Alterado para que os asteroids fiquem na pasta do processo.
        ASTEROID_PATH = current_path.joinpath("asteroids")
        ASTEROID_PATH.mkdir(parents=True, exist_ok=False)

        log.info("Asteroid PATH: [%s]" % ASTEROID_PATH)

        # Parametros usados na Predição de Ocultação
        # predict_start_date: Data de Inicio da Predição no formato "YYYY-MM-DD". Default = NOW()
        # predict_end_date: Data de Termino da Predição no formato "YYYY-MM-DD". Default = NOW() + 1 Year
        # predict_step: Intervalo em segundos que será usado na ephemeris do objeto durante a predição. default = 600
        PREDICT_START = datetime.now()
        if "predict_start_date" in job and "predict_start_date" != None:
            PREDICT_START = datetime.strptime(job["predict_start_date"], "%Y-%m-%d")
        log.info("Predict Start Date: [%s]" % PREDICT_START)

        PREDICT_END = PREDICT_START.replace(year=PREDICT_START.year + 1)
        if "predict_end_date" in job and "predict_end_date" != None:
            PREDICT_END = datetime.strptime(job["predict_end_date"], "%Y-%m-%d")
            PREDICT_END = PREDICT_END.replace(hour=23, minute=59, second=59)
        log.info("Predict End Date: [%s]" % PREDICT_END)

        PREDICT_STEP = int(job.get("predict_step", 600))
        log.info("Predict Step: [%s]" % PREDICT_STEP)

        job.update({
            "predict_start_date": str(PREDICT_START.date()),
            "predict_end_date": str(PREDICT_END.date())
            })

        # TODO: Utilizar os parametros de BSP_PLanetary e LEAP Second do job.json.
        # BSP_PLANETARY = job["bsp_planetary"]["absolute_path"]
        # log.info("BSP_PLANETARY: [%s]" % BSP_PLANETARY)

        # LEAP_SECOND = job["leap_seconds"]["absolute_path"]
        # log.info("LEAP_SECOND: [%s]" % LEAP_SECOND)

        # Remove resultados e inputs de execuções anteriores
        # Durante o desenvolvimento é util não remover os inputs pois acelera o processamento
        # No uso normal é recomendado sempre regerar os inputs
        FORCE_REFRESH_INPUTS = bool(job.get("force_refresh_inputs", True))
        log.info("Force Refresh Inputs: [%s]" % FORCE_REFRESH_INPUTS)

        # Determina a validade dos arquivos de inputs.
        # Durante o desenvolvimento é util não fazer o download a cada execução
        # No uso normal é recomendado sempre baixar os inputs utilizando valor 0 
        inputs_days_to_expire = int(job.get("inputs_days_to_expire", 0))
        BSP_DAYS_TO_EXPIRE = inputs_days_to_expire
        ORBITAL_ELEMENTS_DAYS_TO_EXPIRE = inputs_days_to_expire
        OBSERVATIONS_DAYS_TO_EXPIRE = inputs_days_to_expire
        DES_OBSERVATIONS_DAYS_TO_EXPIRE = inputs_days_to_expire
        log.info("Input days to expire: [%s]" % inputs_days_to_expire)

        # =========================== Asteroids ===========================
        # Retrieve Asteroids.
        log.info("Retriving Asteroids started")

        step_t0 = datetime.now(tz=timezone.utc)

        asteroids = retrieve_asteroids(job["filter_type"], job["filter_value"])

        # asteroids = asteroids[0:5]

        step_t1 = datetime.now(tz=timezone.utc)
        step_tdelta = step_t1 - step_t0

        job.update({"count_asteroids": len(asteroids)})

        setup_job_status(jobid, len(asteroids))

        log.info("Asteroids Count: %s" % job["count_asteroids"])

        log.info(
            "Retriving Asteroids Finished in %s"
            % humanize.naturaldelta(step_tdelta, minimum_unit="microseconds")
        )

        # Update Job File
        update_job(job)

        if job["count_asteroids"] == 0:
            raise ("No asteroid satisfying the criteria %s and %s. There is nothing to run." % (job["filter_type"], job["filter_value"]))


        # Lista de Jobs do Condor.
        htc_jobs = list()

        # Diretório para armazenar os jobs que foram submetidos no HTCondor.
        # Cada job vai gerar um arquivo neste diretório
        # Que depois vai ser verificado pela segunda etapa. 
        # Esses arquivos é que fazem a ligação entre as 2 etapas do pipeline.
        HTC_JOBS_PATH = current_path.joinpath("jobs")
        HTC_JOBS_PATH.mkdir(parents=True, exist_ok=False)

        JOBS_CALLBACK_PATH = current_path.joinpath("callback")
        JOBS_CALLBACK_PATH.mkdir(parents=True, exist_ok=False)

        hb_t0 = datetime.now(tz=timezone.utc)

        for asteroid in asteroids:
            #TODO: Verificar se o job não foi abortado.

            log.info(
                "---------------< Running: %s / %s >---------------"
                % (current_idx, job['count_asteroids'])
            )
            log.info("Asteroid: [%s]" % asteroid["name"])

            a = Asteroid(
                id=asteroid["id"],
                name=asteroid["name"],
                number=asteroid["number"],
                base_dynclass=asteroid["base_dynclass"],
                dynclass=asteroid["dynclass"],
                base_path=ASTEROID_PATH
            )

            a.set_log(logname)

            # Remove Previus Results ----------------------------------
            # Arquivos da execução anterior, resultados e logs por exemplo
            # caso FORCE_REFRESH_INPUTS = TRUE os inputs também serão removidos
            a.remove_previus_results(remove_inputs=FORCE_REFRESH_INPUTS)

            # Observações do DES ----------------------------------
            # Se o objeto não tiver observações no DES
            # ele pode ser executado normalmente mas
            # a etapa de refinamento de orbita será ignorada.
            have_des_obs = a.check_des_observations(
                days_to_expire=DES_OBSERVATIONS_DAYS_TO_EXPIRE
            )

            # ========================= Download dos Inputs Externos ============================
            # BSP JPL -------------------------------------------------------
            # Caso HAJA posições para o DES o BSP precisará ter um periodo inicial que contenham o periodo do DES
            # Para isso basta deixar o bsp_start_date = None e o periodo será setado na hora do download.
            # Se NÃO tiver posições no DES o BSP tera como inicio a data solicitada para predição.
            bsp_start_date = str(PREDICT_START.date())
            if have_des_obs is True:
                bsp_start_date = None

            have_bsp_jpl = a.check_bsp_jpl(
                start_period=bsp_start_date,
                end_period=str(PREDICT_END.date()),
                days_to_expire=BSP_DAYS_TO_EXPIRE,
            )

            if have_bsp_jpl is False:
                log.warning(
                    "Asteroid [%s] Ignored for not having BSP JPL." % asteroid["name"]
                )
                # TODO: guardar informações dos asteroids ignorados e os motivos.

                current_idx += 1
                failures += 1
                # Ignora as proximas etapas para este asteroid.
                continue

            # ORBITAL ELEMENTS ----------------------------------------------
            have_orb_ele = a.check_orbital_elements(
                days_to_expire=ORBITAL_ELEMENTS_DAYS_TO_EXPIRE
            )

            if have_orb_ele is False:
                log.warning(
                    "Asteroid [%s] Ignored for not having Orbital Elements."
                    % asteroid["name"]
                )
                # TODO: guardar informações dos asteroids ignorados e os motivos.
                current_idx += 1
                failures += 1
                # Ignora as proximas etapas para este asteroid.
                continue

            # Observations --------------------------------------------------
            have_obs = a.check_observations(days_to_expire=OBSERVATIONS_DAYS_TO_EXPIRE)

            if have_obs is False:
                log.warning(
                    "Asteroid [%s] Ignored for not having Observations." % asteroid["name"]
                )
                # TODO: guardar informações dos asteroids ignorados e os motivos.

                current_idx += 1
                failures += 1
                # Ignora as proximas etapas para este asteroid.
                continue

            # ========================= Submeter o Job no HTCondor ============================
            log.info("Submitting the Job to HTCondor. [%s]" % str(a.get_path()))

            try:

                # asteroid_path_temp = '/archive/des/tno/dev/asteroids/%s' % asteroid['alias']
                htc_submited = submit_job(
                    name=a.alias,
                    number=a.number,
                    start=str(PREDICT_START.date()),
                    end=str(PREDICT_END.date()),
                    step=PREDICT_STEP,
                    path=a.get_path()
                )

                if htc_submited["success"]:
                    htc_job = htc_submited["jobs"][0]

                    # Associa o Job submetido com o asteroid.
                    htc_job.update({
                        "name": a.name,
                        "alias": a.alias, 
                        "path": a.get_path()})
                    htc_jobs.append(htc_job)
                    # Associa o Asteroid com o Job Submetido.
                    a.set_condor_job(
                        procid=htc_job["ProcId"], clusterid=htc_job["ClusterId"]
                    )

                    # Cria um arquivo que sera utilizado para monitorar o job
                    # Na outra etapa do pipeline.
                    write_htc_job_file(HTC_JOBS_PATH, htc_job)
                    log.info("HTCondor Job submitted. %s" % str(htc_job))

                    success +=1
                else:
                    log.error("Job submission failed on HTCondor. %s" % htc_submited)
                    # TODO: Marcar que deu erro no asteroid na submissão
                    current_idx += 1
                    failures += 1
                    continue

            except Exception as e:
                log.error(e)
                log.error("Job submission failed on HTCondor. %s" % htc_submited)
                # Se der erro na submissão do job continua para o proximo asteroid
                # TODO: Marcar que deu erro no asteroid na submissão
                current_idx += 1
                failures += 1
                continue
            
            # TODO: guardar em um dataset informação do asteroid e associado ao job. e se o asteroid foi executado ou ignorado.
            current_idx += 1
            del a

            update_progress_status(
                jobid, 
                step=1,
                status=2, 
                count=int(job['count_asteroids']),
                current = current_idx - 1,
                success = success,
                failures= failures,
                t0=hb_t0)

        log.info("All jobs have been submitted.")
        df = pd.DataFrame(htc_jobs)
        htc_jobs_filepath = pathlib.Path(current_path, "htc_jobs_submited.csv")
        df.to_csv(htc_jobs_filepath, encoding="utf-8", sep=";", index=False)

        job.update({
            "submited_all_jobs": True,
            "condor_job_submited": len(htc_jobs)
        })
        update_job(job)

        # Fim da etapa de submissão
        update_progress_status(
            jobid, 
            step=1,
            status=3, 
            count=int(job['count_asteroids']),
            current = current_idx -1,
            success = success,
            failures= failures,
            t0=hb_t0)
        
    except Exception as e:
        trace = traceback.format_exc()
        log.error(trace)
        log.error(e)

        # Status 4 = Failed
        job.update(
            {"status": "Failed", "error": str(e), "traceback": str(trace),}
        )

        update_progress_status(
            jobid, 
            step=1,
            status=4, 
            count=int(job['count_asteroids']),
            current = current_idx,
            success = success,
            failures= failures,
            t0=hb_t0)

    finally:
        # Altera o path de execução para o path original
        os.chdir(original_path)        
        # TODO: Escrever um finish
        # finish_job(job)
        log.info("Finish Submit tasks")

def consolidate_job_results(consolidated, job_path):
    print("Consolidating Job Results.")
    # log.info("Consolidating Job Results.")
    df_result = pd.DataFrame(
        consolidated,
        columns=[
            "ast_id", "name", "number", "base_dynclass", "dynclass",
            "des_obs", "des_obs_start", "des_obs_finish", "des_obs_exec_time",
            "des_obs_gen_run", "des_obs_tp_start", "des_obs_tp_finish",
            "bsp_jpl_start", "bsp_jpl_finish", "bsp_jpl_dw_time", 
            "bsp_jpl_dw_run", "bsp_jpl_tp_start", "bsp_jpl_tp_finish",
            "obs_source", "obs_start", "obs_finish", "obs_dw_time", 
            "obs_dw_run", "obs_tp_start", "obs_tp_finish", 
            "orb_ele_source", "orb_ele_start", "orb_ele_finish", "orb_ele_dw_time",
            "orb_ele_dw_run", "orb_ele_tp_start", "orb_ele_tp_finish",
            "ref_orb_start", "ref_orb_finish", "ref_orb_exec_time", 
            "pre_occ_count", "pre_occ_start", "pre_occ_finish", "pre_occ_exec_time",
            "ing_occ_count", "ing_occ_start", "ing_occ_finish", "ing_occ_exec_time",
            "exec_time", "messages", "status"],
        )

    result_filepath = pathlib.Path(job_path, "job_consolidated.csv")
    mode = 'a'
    header = False 

    if not result_filepath.exists():
        mode = 'w'
        header = True
    df_result.to_csv(
        result_filepath, 
        encoding="utf-8", 
        sep=";", 
        index=False,
        mode = mode, 
        header=header
    )
    del df_result
    # log.info("File with the consolidated Job data. [%s]" % result_filepath)

def complete_job(job, log, status):

    consolidated_filepath = pathlib.Path(job.get('path'), "job_consolidated.csv")
    df = pd.read_csv(consolidated_filepath, delimiter=';')

    l_status = df['status'].to_list()
    count_success = int(l_status.count(1))
    count_failures = int(l_status.count(2))
    occultations = int(df['ing_occ_count'].sum())
    ast_with_occ = int((df['ing_occ_count']!=0).sum())
    print(f"Count Success {count_success} Failure {count_failures} Occ {occultations} ast {ast_with_occ}")

    t0 = datetime.fromisoformat(job.get("start"))        
    t1 = datetime.now(tz=timezone.utc)
    tdelta = t1 - t0

    # Calc average time by asteroid
    avg_exec_time_asteroid = 0
    if (job.get('count_asteroids') > 0):
        avg_exec_time_asteroid = int(tdelta.total_seconds() / job.get('count_asteroids'))

    # Status 3 = Completed
    job.update({
        "status": status,
        "ast_with_occ": ast_with_occ,
        "occultations": occultations,
        "count_success": count_success,
        "count_failures": count_failures,
        "end": t1.isoformat(), 
        "exec_time": tdelta.total_seconds(),
        "h_exec_time": humanize.naturaldelta(tdelta),
        "avg_exec_time": avg_exec_time_asteroid,
    })

    log.info("Update Job status.")
    # write_job_file(current_path, job)
    update_job(job)

    # Remove o diretório de asteroids do job. 
    if not job['debug']:
        log.debug("Removing asteroid directory.")
        asteroid_path = pathlib.Path(job['path']).joinpath("asteroids")
        shutil.rmtree(asteroid_path)
        log.info("Directory of asteroids has been removed!")

    log.info(
        "Asteroids Success: [%s] Failure: [%s] Total: [%s]"
        % (job["count_success"], job["count_failures"], job["count_asteroids"])
    )

    log.info("Execution Time: %s" % tdelta)
    log.info("Predict Occultation is done!.")

def mark_task_to_be_removed(cluster_id: int, proc_id: int):
    print(f"To be Removed: {cluster_id} - {proc_id}")
    condor_m = Condor()
    condor_m.remove_job(
        clusterId=cluster_id,
        procId=proc_id
    )

def check_task_time_out(htc_job, time_limit:int=30):

    if 'JobStartDate' not in htc_job:
        print("Não retornou o jobstartdate investigar o erro.")
        # Retornar como se estivesse excedido o tempo
        # para que o job seja removido.
        return True
    
    # Verifica se o tempo de execução é maior que limite
    htc_job_start = datetime.fromtimestamp(
        int(htc_job["JobStartDate"])
    )
    now = datetime.now()
    running_time = now - htc_job_start
    running_minutes = running_time.seconds / 60.0

    if running_minutes > time_limit:
        # Job excedeu o tempo limite será marcado para remoção.
        print("Job excedeu o tempo limite será marcado para remoção.")
        return True

    return False


def handle_htcondor_task_status(htc_job, log, time_limit:int=30):
    status = int(htc_job['JobStatus'])
    print(f"Handle task with status: {status}")
    time_limit = 30

    # Job Idle
    if status == 1:
        return dict({'completed': False})

    # Job Running
    if status == 2:
        # Check Time out
        exceeded = check_task_time_out(htc_job, time_limit)
        if exceeded:
            mark_task_to_be_removed(
                int(htc_job["ClusterId"]), 
                int(htc_job["ProcId"])) 
            log.warning(
                "Job timed out and will be removed. [%s]"
                % htc_job["ClusterId"]
            )
        return dict({'completed': False})
    
    # Job Removed
    if status == 3:
        log.debug(f"Job {htc_job['ClusterId']} has been removed.")                
        return dict({
            'completed': True,
            'success': False
        })

    # Job Completed
    if status == 4:
        log.info("Job Completed: [%s]" % htc_job["ClusterId"])
        return dict({
            'completed': True,
            'success': True
        })

    # Job Held/Hold
    if status == 5:
        mark_task_to_be_removed(
            int(htc_job["ClusterId"]), 
            int(htc_job["ProcId"]))
        
        msg = f"Job {htc_job['ClusterId']} in Hold has been marked for removal."
        log.warning(msg)
        return dict({
            'completed': False,
            'message': msg
        })

    # Job Status não esperado marcar para remover
    if status not in [1,2,3,4,5]:
        mark_task_to_be_removed(
            int(htc_job["ClusterId"]), 
            int(htc_job["ProcId"]))     
        
        msg = f"Job {htc_job['ClusterId']} with Unexpected Status {status}. has been marked for removal."
        return dict({
            'completed': False,
            'message': msg
        })

def check_condor_status(cluster_id, proc_id):
    try:
        condor_m = Condor()
        return condor_m.get_job(
            clusterId=cluster_id, procId=proc_id
        )
    except Exception as e:
        raise Exception(f"Falhou ao consultar o status do job no condor. {e}")

def check_tasks(jobid):
    config = get_configs()

    # Read Inputs from job.json
    job = read_job_json_by_id(jobid)

    if 'check_tasks' not in job or job['check_tasks'] is None:
        job.update({
            "check_tasks": {
                "start": datetime.now(tz=timezone.utc).isoformat(),
            }
        })

    # Paths de execução
    original_path = os.getcwd()
    current_path = pathlib.Path(job.get('path'))

    DEBUG = job.get("debug", False)

    # Create a Log file
    logname = "check_tasks"
    log = get_logger(current_path, f"{logname}.log", DEBUG)

    ASTEROID_PATH = current_path.joinpath("asteroids")

    # Diretório para armazenar os jobs que foram submetidos no HTCondor.
    # Cada job vai gerar um arquivo neste diretório
    # Que depois vai ser verificado pela segunda etapa. 
    # Esses arquivos é que fazem a ligação entre as 2 etapas do pipeline.
    HTC_JOBS_PATH = current_path.joinpath("jobs")
    if not HTC_JOBS_PATH.exists():
        return

    # CONDOR_JOB_TIME_LIMIT: Tempo Limite de execução de um job no HTCondor em Minutos.
    # Todo Job submetido e que estiver com JobStatus = 2 ou seja Running
    # será verificado o tempo de execução contando a partir do JobStartDate caso o tempo de execução seja maior
    # que o determinado nesta variavel ele será marcado para remoção.
    # default = 20
    CONDOR_JOB_TIME_LIMIT = int(config["DEFAULT"].getint("CondorJobTimeLimit", 20))
    log.debug("Condor Job time Limit: [%s]" % CONDOR_JOB_TIME_LIMIT)

    htc_jobs = list()
    pathlist = HTC_JOBS_PATH.glob('*.json')

    # Enquanto a etapa submit ainda está executando
    # considera o total de asteroids como sendo o total de jobs. 
    # Apos a etapa submit ser finalizada o total desta etapa passa a ser o total
    # de jobs que foram submetidos.
    count_tasks = int(job['count_asteroids'])
    if job['submited_all_jobs']:
        count_tasks = int(job['condor_job_submited'])

    current_idx = (job['condor_job_completed'] + job['condor_job_removed'])
    if current_idx == 0:
        current_idx += 1

    l_consolidated = list()

    # Para cada task verificar direto no diretório do asteroid
    # Se foi gerado o arquivo com as listas de ocultações. 
    # a existencia deste arquivo determina que a tarefa esta completa.
    for job_file in pathlist:
        htc_job = read_htc_job_file(job_file)

        a = Asteroid(name=htc_job['name'], base_path=ASTEROID_PATH)
        a.set_log(log)

        # checar o status do job no condor aqui. 
        status = check_condor_status(
            cluster_id=htc_job["ClusterId"], 
            proc_id=htc_job["ProcId"])

        htc_job.update(status)
        print("Condor status: " + htc_job['JobStatus'])       
        result = handle_htcondor_task_status(htc_job, log, CONDOR_JOB_TIME_LIMIT)
        print(result)

        # TODO: Rever essa parte.
        if 'message' in result:
            htc_job.update({"_message": result['message']})
      
        # if result['completed'] == True and result['success'] == True:
        #     # Job completo com sucesso.
        #     job['condor_job_completed'] += 1
        #     fp = write_htc_job_file(HTC_JOBS_PATH, htc_job)
        #     # Troca a extensão do arquivo para impedir que ele seja processado denovo.
        #     fp.rename(fp.with_suffix('.completed'))

        #     # Registrar as predições no banco de dados
        #     # Se o Asteroid tiver pelo menos um evento de occultação
        #     # Verifica o arquivo de resultados da predição.
        #     # Insere os resultados no database
        #     ingested_occ_count = a.register_occultations(
        #         job.get('predict_start_date'),
        #         job.get('predict_end_date')
        #     )
        #     log.info("Asteroid: [%s] Occultations: [%s]" % (a.name, ingested_occ_count))

        if result['completed'] == True:
            htc_jobs.append(htc_job)

            fp = write_htc_job_file(HTC_JOBS_PATH, htc_job)

            if result['success'] == True:
                # Job completo com sucesso.
                job['condor_job_completed'] += 1
                # Troca a extensão do arquivo para impedir que ele seja processado denovo.
                fp.rename(fp.with_suffix('.completed'))

                # Registrar as predições no banco de dados
                # Se o Asteroid tiver pelo menos um evento de occultação
                # Verifica o arquivo de resultados da predição.
                # Insere os resultados no database
                ingested_occ_count = a.register_occultations(
                    job.get('predict_start_date'),
                    job.get('predict_end_date')
                )
                log.info("Asteroid: [%s] Occultations: [%s]" % (a.name, ingested_occ_count))

            if result['success'] == False:
                # Job Removed.
                job['condor_job_removed'] += 1
                # Troca a extensão do arquivo para impedir que ele seja processado denovo.
                fp.rename(fp.with_suffix('.removed'))

            # Aproveita o Loop em asteroids para gerar um resumo consolidado
            # de todos os asteroids envolvidos no Job
            consolidated = a.consiladate()
            l_consolidated.append(consolidated)
            
            # Remove todos os arquivos do asteroid gerados durante o processo, 
            # deixa apenas os inputs
            if not DEBUG:
                a.remove_outputs()
                del a

            print("Task completed - Update Progress bar step2")
            update_progress_status(
                jobid, 
                step=2,
                status=2, 
                count=count_tasks,
                current = current_idx,
                success = job['condor_job_completed'],
                failures= job['condor_job_removed'],
                t0=datetime.fromisoformat(job['check_tasks']['start']))        

            current_idx += 1

    update_job(job)

    # ========================= Consolidando resultados ============================
    # Fazer um consolidate parcial.
    if len(l_consolidated) > 0:
        print("l_consolidate > 0")
        consolidate_job_results(l_consolidated, current_path)

    jobs_finished = job['condor_job_completed'] + job['condor_job_removed']
    if jobs_finished == job['condor_job_submited']:
        try:
            print("Job completed - Update Progress bar step2")
            update_progress_status(
                jobid, 
                step=2,
                status=3, 
                count=count_tasks,
                current = current_idx,
                success = job['condor_job_completed'],
                failures= job['condor_job_removed'],
                t0=datetime.fromisoformat(job['check_tasks']['start']))

            log.info("Ingest Predict Occultation Job Results in database")
            count_results_ingested = ingest_job_results(current_path, jobid)
            log.debug("Predict Occultation Job Results ingested: %s" % count_results_ingested)

            complete_job(job, log , "Completed")
            return True
        
        except Exception as e:
            msg = f"Falhou na etapa de ingestão dos resultados. {e}"
            raise Exception(e)
        







# def check_condor_jobs(jobid):
#     config = get_configs()

#     # Read Inputs from job.json
#     job = read_job_json_by_id(jobid)

#     # Paths de execução
#     original_path = os.getcwd()
#     current_path = pathlib.Path(job.get('path'))

#     DEBUG = job.get("debug", False)

#     # Create a Log file
#     logname = "check_tasks"
#     log = get_logger(current_path, f"{logname}.log", DEBUG)

#     # CONDOR_JOB_TIME_LIMIT: Tempo Limite de execução de um job no HTCondor em Minutos.
#     # Todo Job submetido e que estiver com JobStatus = 2 ou seja Running
#     # será verificado o tempo de execução contando a partir do JobStartDate caso o tempo de execução seja maior
#     # que o determinado nesta variavel ele será marcado para remoção.
#     # default = 20
#     CONDOR_JOB_TIME_LIMIT = int(config["DEFAULT"].getint("CondorJobTimeLimit", 20))
#     log.info("Condor Job time Limit: [%s]" % CONDOR_JOB_TIME_LIMIT)

#     # Diretório para armazenar os jobs que foram submetidos no HTCondor.
#     # Cada job vai gerar um arquivo neste diretório
#     # Que depois vai ser verificado pela segunda etapa. 
#     # Esses arquivos é que fazem a ligação entre as 2 etapas do pipeline.
#     HTC_JOBS_PATH = current_path.joinpath("jobs")
#     if not HTC_JOBS_PATH.exists():
#         return

#     # TODO: Recuperar o total de htcondor jobs.
#     htc_jobs_filepath = pathlib.Path(current_path, "htc_jobs_submited.csv")
#     htc_jobs = list()

#     # O arquivo com a lista de jobs submited só é criado no final da primeira etapa.
#     # Enquanto a primeira etapa não finaliza, utilizar os arquivos criados na pasta jobs.
#     if htc_jobs_filepath.exists():
#         df = pd.read_csv(
#             htc_jobs_filepath, 
#             delimiter=";")
#         htc_jobs = df.to_dict('records')
#     else:
#         pathlist = HTC_JOBS_PATH.glob('*.json')
#         for path in pathlist:
#             htc_jobs.append(read_htc_job_file(path))

#     print(htc_jobs)

#     # ========================= Verifica todos os Jobs no HTCondor ============================
#     try:
#         condor_m = Condor()

#         total_jobs = len(htc_jobs)
#         finished_jobs = list()
#         finished_ids = list()
#         removed_jobs = list()
#         count_idle = 0
#         count_running = 0

#         for htc_job in htc_jobs:
#             if htc_job["ClusterId"] in finished_ids:
#                 # Ignora as proximas etapas para este job.
#                 # Se ele já estiver completo
#                 continue

#             try:
#                 status = condor_m.get_job(
#                     clusterId=htc_job["ClusterId"], procId=htc_job["ProcId"]
#                 )
#                 print(status)
#                 if "JobStatus" in status:

#                     if status["JobStatus"] == "1":
#                         # Job Idle
#                         count_idle += 1

#                     elif status["JobStatus"] == "2":
#                         # Job Running
#                         try:
#                             # Verifica se o tempo de execução é maior que limite
#                             htc_job_start = datetime.fromtimestamp(
#                                 int(status["JobStartDate"])
#                             )
#                             now = datetime.now()
#                             running_time = now - htc_job_start
#                             # Converte o delta time para minutos
#                             running_minutes = running_time.seconds % 3600 / 60.0

#                             if running_minutes > int(CONDOR_JOB_TIME_LIMIT):
#                                 # Job excedeu o tempo limite será marcado para remoção.
#                                 condor_m.remove_job(
#                                     clusterId=htc_job["ClusterId"],
#                                     procId=htc_job["ProcId"],
#                                 )
#                                 log.warning(
#                                     "Job timed out and will be removed. [%s]"
#                                     % htc_job["ClusterId"]
#                                 )

#                             count_running += 1
#                         except Exception as e:
#                             log.error("Não retornou JobStartDate")
#                             log.error(status)
#                             log.error(e)

#                             # TODO: Este remove é temporario só para identificar o problema
#                             condor_m.remove_job(
#                                 clusterId=htc_job["ClusterId"], procId=htc_job["ProcId"]
#                             )

#                     elif status["JobStatus"] == "3":
#                         # Job Foi removido
#                         finished_jobs.append(status)
#                         finished_ids.append(htc_job["ClusterId"])
#                         removed_jobs.append(htc_job)

#                         log.info("job has been removed. [%s]" % htc_job["ClusterId"])

#                     elif status["JobStatus"] == "4":
#                         finished_jobs.append(status)
#                         finished_ids.append(htc_job["ClusterId"])
#                         log.info("Job Completed: [%s]" % status["ClusterId"])

#                     elif status["JobStatus"] == "5":
#                         # Job Held/Hold
#                         # Marcar o Job para Remoção
#                         condor_m.remove_job(
#                             clusterId=htc_job["ClusterId"], procId=htc_job["ProcId"]
#                         )
#                         log.warning(
#                             "Job in Hold has been marked for removal. [%s]"
#                             % htc_job["ClusterId"]
#                         )
#                         # Não adicionar o job ao terminados só quando ele for removido.

#                     else:
#                         # Job Status não esperado marcar para remover
#                         # Marcar o Job para Remoção
#                         condor_m.remove_job(
#                             clusterId=htc_job["ClusterId"], procId=htc_job["ProcId"]
#                         )
#                         log.warning(
#                             "Job [%s] with Unexpected Status. has been marked for removal. [%s]"
#                             % (htc_job["ClusterId"], status)
#                         )
#                         # Não adicionar o job ao terminados só quando ele for removido.

#             except Exception as e:
#                 log.error("Falhou ao consultar o condor")
#                 log.error(e)

#         log.info(
#             "Total: Jobs [%s] Completed [%s] Running [%s] Idle [%s]."
#             % (total_jobs, len(finished_jobs), count_running, count_idle)
#         )
#         count_idle = 0
#         count_running = 0

#         # Total de Jobs completos e removidos
#         condor_job_completed = len(finished_jobs)
#         condor_job_removed = len(removed_jobs)

#         # Todos os jobs acabaram gerar um csv com os dados dos jobs retornados pelo HTCondor
#         # Apenas para debug caso necessário.
#         # log.info("All Condor Jobs are done.")
#         # log.info("Jobs Removed : [%s]" % len(removed_jobs))
#         # df = pd.DataFrame(finished_jobs)
#         # htc_jobs_filepath = pathlib.Path(current_path, "htc_jobs.csv")
#         # df.to_csv(htc_jobs_filepath, encoding="utf-8", sep=";", index=False)
#         # del finished_jobs

#     except Exception as e:
#         trace = traceback.format_exc()
#         log.error(trace)
#         log.error(e)

#         # Status 4 = Failed
#         job.update(
#             {"status": "Failed", "error": str(e), "traceback": str(trace)}
#         )
#         update_job(job)

#     finally:
#         log.info("Fim da checagem dos jobs.")



#     #     # ========================= Verifica todos os Jobs no HTCondor ============================
#     # try:
#     #     condor_m = Condor()

#     #     total_jobs = len(htc_jobs)
#     #     finished_jobs = list()
#     #     finished_ids = list()
#     #     removed_jobs = list()
#     #     count_idle = 0
#     #     count_running = 0

# #         while len(finished_jobs) < total_jobs:

# #             # Sleep Necessário
# #             time.sleep(30)

# #             for htc_job in htc_jobs:

# #                 if htc_job["ClusterId"] in finished_ids:
# #                     # Ignora as proximas etapas para este job.
# #                     # Se ele já estiver completo
# #                     continue

# #                 try:
# #                     status = condor_m.get_job(
# #                         clusterId=htc_job["ClusterId"], procId=htc_job["ProcId"]
# #                     )

# #                     if "JobStatus" in status:

# #                         if status["JobStatus"] == "1":
# #                             # Job Idle
# #                             count_idle += 1

# #                         elif status["JobStatus"] == "2":
# #                             # Job Running
# #                             try:
# #                                 # Verifica se o tempo de execução é maior que limite
# #                                 htc_job_start = datetime.fromtimestamp(
# #                                     int(status["JobStartDate"])
# #                                 )
# #                                 now = datetime.now()
# #                                 running_time = now - htc_job_start
# #                                 # Converte o delta time para minutos
# #                                 running_minutes = running_time.seconds % 3600 / 60.0

# #                                 if running_minutes > int(CONDOR_JOB_TIME_LIMIT):
# #                                     # Job excedeu o tempo limite será marcado para remoção.
# #                                     condor_m.remove_job(
# #                                         clusterId=htc_job["ClusterId"],
# #                                         procId=htc_job["ProcId"],
# #                                     )
# #                                     log.warning(
# #                                         "Job timed out and will be removed. [%s]"
# #                                         % htc_job["ClusterId"]
# #                                     )

# #                                 count_running += 1
# #                             except Exception as e:
# #                                 log.error("Não retornou JobStartDate")
# #                                 log.error(status)
# #                                 log.error(e)

# #                                 # TODO: Este remove é temporario só para identificar o problema
# #                                 condor_m.remove_job(
# #                                     clusterId=htc_job["ClusterId"], procId=htc_job["ProcId"]
# #                                 )

# #                         elif status["JobStatus"] == "3":
# #                             # Job Foi removido
# #                             finished_jobs.append(status)
# #                             finished_ids.append(htc_job["ClusterId"])
# #                             removed_jobs.append(htc_job)

# #                             log.debug("job has been removed. [%s]" % htc_job["ClusterId"])

# #                         elif status["JobStatus"] == "4":
# #                             finished_jobs.append(status)
# #                             finished_ids.append(htc_job["ClusterId"])
# #                             log.debug("Job Completed: [%s]" % status["ClusterId"])

# #                         elif status["JobStatus"] == "5":
# #                             # Job Held/Hold
# #                             # Marcar o Job para Remoção
# #                             condor_m.remove_job(
# #                                 clusterId=htc_job["ClusterId"], procId=htc_job["ProcId"]
# #                             )
# #                             log.warning(
# #                                 "Job in Hold has been marked for removal. [%s]"
# #                                 % htc_job["ClusterId"]
# #                             )
# #                             # Não adicionar o job ao terminados só quando ele for removido.

# #                         else:
# #                             # Job Status não esperado marcar para remover
# #                             # Marcar o Job para Remoção
# #                             condor_m.remove_job(
# #                                 clusterId=htc_job["ClusterId"], procId=htc_job["ProcId"]
# #                             )
# #                             log.warning(
# #                                 "Job [%s] with Unexpected Status. has been marked for removal. [%s]"
# #                                 % (htc_job["ClusterId"], status)
# #                             )
# #                             # Não adicionar o job ao terminados só quando ele for removido.

# #                 except Exception as e:
# #                     log.error("Falhou ao consultar o condor")
# #                     log.error(e)

# #             log.info(
# #                 "Total: Jobs [%s] Completed [%s] Running [%s] Idle [%s]."
# #                 % (total_jobs, len(finished_jobs), count_running, count_idle)
# #             )
# #             count_idle = 0
# #             count_running = 0

# #         # Total de Jobs completos e removidos
# #         condor_job_completed = len(finished_jobs)
# #         condor_job_removed = len(removed_jobs)

# #         # Todos os jobs acabaram gerar um csv com os dados dos jobs retornados pelo HTCondor
# #         # Apenas para debug caso necessário.
# #         log.info("All Condor Jobs are done.")
# #         log.info("Jobs Removed : [%s]" % len(removed_jobs))
# #         df = pd.DataFrame(finished_jobs)
# #         htc_jobs_filepath = pathlib.Path(current_path, "htc_jobs.csv")
# #         df.to_csv(htc_jobs_filepath, encoding="utf-8", sep=";", index=False)
# #         del finished_jobs

# #         # ========================= Importacao dos resultados ============================
# #         log.info("------------------------------------------------------------")
# #         log.info("Start Importing Results.")

# #         l_consolidated = list()

# #         # Total de predições que forma ingeridas na tabela
# #         total_occultations = 0
# #         # Total de asteroids com algum evendo de ocultação no periodo.
# #         total_ast_occ = 0

# #         l_status = list()

# #         current_idx = 1
# #         for asteroid in asteroids:

# #             a = Asteroid(
# #                 id=asteroid["id"], name=asteroid["name"], number=asteroid["number"],
# #             )
# #             a.set_log(logname)

# #             # Registrar as predições no banco de dados
# #             # Se o Asteroid tiver pelo menos um evento de occultação
# #             if a.predict_occultation is not None and a.predict_occultation["count"] > 0:

# #                 # TODO: Coletar o tempo da execução da ingestão
# #                 rowcount = a.register_occultations(PREDICT_START.date(), PREDICT_END.date())

# #                 log.info("Asteroid: [%s] Occultations: [%s]" % (asteroid["name"], rowcount))

# #                 total_occultations += rowcount
# #                 total_ast_occ += 1

# #             # Aproveita o Loop em todos os asteroids para gerar um resumo consolidado de todos os asteroids envolvidos no Job
# #             l_consolidated.append(a.consiladate())

# #             l_status.append(a.status)

# #             #  Remove todos os arquivos gerados durante o processo, deixa apenas os inputs
# #             if not DEBUG:
# #                 a.remove_outputs()

# #                 del a

# #             current_idx += 1

# #         log.info(
# #             "Asteroids with Occultations: [%s] Occultations: [%s]"
# #             % (total_ast_occ, total_occultations)
# #         )
# #         count_success = l_status.count(1)
# #         count_failures = l_status.count(2)

# #         # ========================= Consolidando resultados ============================
# #         log.info("Consolidating Job Results.")

# #         df_result = pd.DataFrame(
# #             l_consolidated,
# #             columns=[
# #                 "ast_id",
# #                 "name",
# #                 "number",
# #                 "base_dynclass",
# #                 "dynclass",
# #                 "des_obs",
# #                 "des_obs_start",
# #                 "des_obs_finish",
# #                 "des_obs_exec_time",
# #                 "des_obs_gen_run",
# #                 "des_obs_tp_start",
# #                 "des_obs_tp_finish",
# #                 "bsp_jpl_start",
# #                 "bsp_jpl_finish",
# #                 "bsp_jpl_dw_time",
# #                 "bsp_jpl_dw_run",
# #                 "bsp_jpl_tp_start",
# #                 "bsp_jpl_tp_finish",
# #                 "obs_source",
# #                 "obs_start",
# #                 "obs_finish",
# #                 "obs_dw_time",
# #                 "obs_dw_run",
# #                 "obs_tp_start",
# #                 "obs_tp_finish",
# #                 "orb_ele_source",
# #                 "orb_ele_start",
# #                 "orb_ele_finish",
# #                 "orb_ele_dw_time",
# #                 "orb_ele_dw_run",
# #                 "orb_ele_tp_start",
# #                 "orb_ele_tp_finish",
# #                 "ref_orb_start",
# #                 "ref_orb_finish",
# #                 "ref_orb_exec_time",
# #                 "pre_occ_count",
# #                 "pre_occ_start",
# #                 "pre_occ_finish",
# #                 "pre_occ_exec_time",
# #                 "ing_occ_count",
# #                 "ing_occ_start",
# #                 "ing_occ_finish",
# #                 "ing_occ_exec_time",
# #                 "exec_time",
# #                 "messages",
# #                 "status"
# #             ],
# #         )

# #         result_filepath = pathlib.Path(current_path, "job_consolidated.csv")
# #         df_result.to_csv(result_filepath, encoding="utf-8", sep=";", index=False)
# #         del df_result
# #         log.info("File with the consolidated Job data. [%s]" % result_filepath)

# #         log.info("Ingest Predict Occultation Job Results in database")
# #         count_results_ingested = ingest_job_results(current_path, jobid)
# #         log.debug("Predict Occultation Job Results ingested: %s" % count_results_ingested)

      
# #         # Status 3 = Completed
# #         job.update(
# #             {
# #                 "status": "Completed",
# #                 "ast_with_occ": total_ast_occ,
# #                 "occultations": total_occultations,
# #                 "count_success": count_success,
# #                 "count_failures": count_failures,
# #                 "condor_job_submited": total_jobs,
# #                 "condor_job_completed": condor_job_completed,
# #                 "condor_job_removed": condor_job_removed,
# #             }
# #         )

# #     except Exception as e:
# #         trace = traceback.format_exc()
# #         log.error(trace)
# #         log.error(e)

# #         # Status 4 = Failed
# #         job.update(
# #             {"status": "Failed", "error": str(e), "traceback": str(trace),}
# #         )
# #         update_job(job)

# #     finally:
# #         finish_job(job)

# # def finish_job(job):
# #     t1 = datetime.now(tz=timezone.utc)
# #     tdelta = t1 - t0

# #     # Calc average time by asteroid
# #     avg_exec_time_asteroid = 0
# #     if (job.get('count_asteroids') > 0):
# #         avg_exec_time_asteroid = int(tdelta.total_seconds() / job.get('count_asteroids'))

# #     job.update({
# #         "end": t1.isoformat(), 
# #         "exec_time": tdelta.total_seconds(),
# #         "h_exec_time": humanize.naturaldelta(tdelta),
# #         "avg_exec_time": avg_exec_time_asteroid,
# #     })

# #     log.info("Update Job status.")
# #     # write_job_file(current_path, job)
# #     update_job(job)

# #     # Remove o diretório de asteroids do job. 
# #     if not job['debug']:
# #         log.debug("Removing asteroid directory.")
# #         asteroid_path = pathlib.Path(job['path']).joinpath("asteroids")
# #         shutil.rmtree(asteroid_path)
# #         log.info("Directory of asteroids has been removed!")

# #     log.info(
# #         "Asteroids Success: [%s] Failure: [%s] Total: [%s]"
# #         % (job["count_success"], job["count_failures"], job["count_asteroids"])
# #     )

# #     log.info("Execution Time: %s" % tdelta)
# #     log.info("Predict Occultation is done!.")


# #     # Altera o path de execução para o path original
# #     os.chdir(original_path)


# # Exemplo de execução do script
# # python predict_occultation.py /lustre/t1/tmp/tno/predict_occultation/<job_folder>

# # Exemplo de job.json
# #{
# #	"id": 6,
# #	"status": "Submited",
# #	"submit_time": "2023-03-24T10:20:00",
# #	"path": "/lustre/t1/tmp/tno/predict_occultation/6_Eris",
# #	"filter_type": "name",
# #	"filter_value": "Eris",
# #	"predict_start_date": "2023-01-01",
# #	"predict_end_date": "2023-12-31",
# #	"predict_step": 600,
# #	"force_refresh_inputs": true,
# #	"inputs_days_to_expire": 0,
# #	"debug": true,
# #	"bsp_planetary": {
# #		"name": "de440",
# #		"filename": "de440.bsp",
# #		"absolute_path": "/lustre/t1/tmp/tno/bsp_planetary/de440.bsp"
# #	},
# #	"leap_seconds": {
# #		"name": "naif0012",
# #		"filename": "naif0012.tls",
# #		"absolute_path": "/lustre/t1/tmp/tno/leap_seconds/naif0012.tls"
# #	}
# #}

# if __name__ == '__main__':
#     parser = argparse.ArgumentParser()
#     parser.add_argument("path", help="Job Path")
#     args = parser.parse_args()

    # sys.exit(main(args.path))
# -*- coding: utf-8 -*-

import os
import time
from datetime import datetime, timezone
import traceback
import pathlib
import json
import humanize
import pathlib
import pandas as pd
import configparser
from condor import Condor
import pandas as pd
from asteroid import Asteroid
from dao import (
    PredictOccultationJobDao,
    PredictOccultationJobResultDao,
    PredictOccultationJobStatusDao,
)
from io import StringIO
from library import get_configs
import shutil

from library import (
    get_logger,
    read_inputs,
    write_job_file,
    retrieve_asteroids,
    submit_job,
)

try:
    from app import run_pipeline
    from parsl_config import get_config
except Exception as error:
    print('Error: %s' % str(error))
    raise('Predict Occultation pipeline not installed!')

import parsl


class AbortError(Exception):
    """Treatment when the task is aborted."""

    def __init__(self, message=None, exitcode=-1):
        self.message = message
        self.exitcode = exitcode

        if not self.message:
            self.message = "The process is aborted!"

    def __str__(self):
        return str(self.message)


def job_to_run():
    """Retorna o job com status=1 Idle mas antigo.

    Returns:
        job: Orbit Trace Job model
    """
    dao = PredictOccultationJobDao()
    job = dao.get_job_by_status(1)

    return job


def job_by_id(jobid):
    """Retorna o job pelo jobid.

    Returns:
        job: Orbit Trace Job model
    """
    dao = PredictOccultationJobDao()
    return dao.get_job_by_id(jobid)


def check_abort_job(jobid) -> bool:
    """Checa se o job esta marcado para abortar

    Returns:
        bool: retorna se o job True se o job foi abortado, False se não foi
    """
    job = job_by_id(jobid)

    # status 7 = aborting
    return True if job["status"] == 7 else False


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
        return job.get("id")


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


def make_job_json_file(job, path):
    job_data = dict(
        {
            "id": job.get("id"),
            "status": "Idle",
            "submit_time": job.get("submit_time").astimezone(timezone.utc).isoformat(),
            "estimated_execution_time": str(job.get("estimated_execution_time")),
            "path": str(path),
            "filter_type": job.get("filter_type"),
            "filter_value": job.get("filter_value"),
            "predict_start_date": job.get("predict_start_date").isoformat(),
            "predict_end_date": job.get("predict_end_date").isoformat(),
            "predict_step": job.get("predict_step", 600),
            "debug": bool(job.get("debug", False)),
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
        }
    )

    write_job_file(path, job_data)
    update_job(job_data)


def remove_job_directory(jobid):
    # Apaga o diretorio usando rm pq o diretorio tem links simbolicos.
    job_path = get_job_path(jobid)
    print(f"Clear Job path: [{job_path}]")
    try:
        shutil.rmtree(job_path, ignore_errors=False)
    except:
        cmd = f"rm -rf {job_path}"
        print(f"Removeu diretorio: [{cmd}]")
        os.system(cmd)


def get_job_path(jobid):
    """Retorna o path para o diretorio do job, cria o diretorio caso nao exista.
    """
    config = get_configs()
    orbit_trace_root = config["DEFAULT"].get("PredictOccultationJobPath")

    folder_name = f"{jobid}"
    # folder_name = f"teste_{job['id']}"
    # folder_name = f"{job['id']}-{str(uuid.uuid4())[:8]}"

    job_path = pathlib.Path(orbit_trace_root).joinpath(folder_name)

    if not job_path.exists():
        job_path.mkdir(parents=True, mode=0o775)

    print(f"Job Path: {job_path}")
    return job_path


def rerun_job(jobid: int):
    """Apaga os dados no DB referente ao job e remove o diretorio.
    Altera o status do Job para idle.
    """
    print(f"Rerun Job: [{jobid}]")
    daojob = PredictOccultationJobDao()

    # Faz um update na tabela de job zerando os campos.
    daojob.development_reset_job(jobid)

    daoresult = PredictOccultationJobResultDao()
    daoresult.delete_by_job_id(jobid)

    daostatus = PredictOccultationJobStatusDao()
    daostatus.delete_by_job_id(jobid)

    # Clear Directory
    remove_job_directory(jobid)

    run_job(jobid)


def update_job(job) -> None:
    dao = PredictOccultationJobDao()
    dao.update_job(job)

    write_job_file(job.get("path"), job)


def ingest_job_results(job_path, job_id):
    dao = PredictOccultationJobResultDao()
    dao.delete_by_job_id(job_id)

    filepath = pathlib.Path(job_path, "job_consolidated.csv")

    df = pd.read_csv(
        filepath,
        delimiter=";",
        usecols=[
            "ast_id",
            "name",
            "number",
            "base_dynclass",
            "dynclass",
            "des_obs",
            "obs_source",
            "orb_ele_source",
            "pre_occ_count",
            "ing_occ_count",
            "messages",
            "exec_time",
            "status",
            "des_obs_start",
            "des_obs_finish",
            "des_obs_exec_time",
            "bsp_jpl_start",
            "bsp_jpl_finish",
            "bsp_jpl_dw_time",
            "obs_start",
            "obs_finish",
            "obs_dw_time",
            "orb_ele_start",
            "orb_ele_finish",
            "orb_ele_dw_time",
            "ref_orb_start",
            "ref_orb_finish",
            "ref_orb_exec_time",
            "pre_occ_start",
            "pre_occ_finish",
            "pre_occ_exec_time",
            "ing_occ_start",
            "ing_occ_finish",
            "ing_occ_exec_time",
        ],
    )
    df["job_id"] = int(job_id)
    df = df.rename(columns={"ast_id": "asteroid_id", "pre_occ_count": "occultations"})

    df["des_obs"].fillna(0, inplace=True)
    df["occultations"].fillna(0, inplace=True)
    df["ing_occ_count"].fillna(0, inplace=True)

    df = df.astype(
        {
            "des_obs": "int32",
            "occultations": "int32",
            "ing_occ_count": "int32",
            "asteroid_id": "int32",
            "job_id": "int32",
            "status": "int32",
        }
    )

    df = df.reindex(
        columns=[
            "name",
            "number",
            "base_dynclass",
            "dynclass",
            "status",
            "des_obs",
            "obs_source",
            "orb_ele_source",
            "occultations",
            "ing_occ_count",
            "exec_time",
            "messages",
            "asteroid_id",
            "job_id",
            "des_obs_start",
            "des_obs_finish",
            "des_obs_exec_time",
            "bsp_jpl_start",
            "bsp_jpl_finish",
            "bsp_jpl_dw_time",
            "obs_start",
            "obs_finish",
            "obs_dw_time",
            "orb_ele_start",
            "orb_ele_finish",
            "orb_ele_dw_time",
            "ref_orb_start",
            "ref_orb_finish",
            "ref_orb_exec_time",
            "pre_occ_start",
            "pre_occ_finish",
            "pre_occ_exec_time",
            "ing_occ_start",
            "ing_occ_finish",
            "ing_occ_exec_time",
        ]
    )

    data = StringIO()
    df.to_csv(
        data,
        sep="|",
        header=True,
        index=False,
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
    return read_inputs(job_db["path"], "job.json")


def update_progress_status(
    job_id: int,
    step: int,
    t0: datetime,
    status: int = None,
    count: int = 0,
    current: int = 0,
    success: int = 0,
    failures: int = 0,
):
    t1 = datetime.now(tz=timezone.utc)
    tdelta = t1 - t0
    tdelta = tdelta.total_seconds()
    average_time = tdelta / count
    time_estimate = tdelta * (count - current)

    tasks = ["Data acquisition and preparation", "Refine Orbit and Predict Occultation"]
    task = tasks[step - 1]

    dao = PredictOccultationJobStatusDao()
    dao.update_or_insert(
        job_id,
        step,
        task,
        status,
        count,
        current,
        average_time,
        time_estimate,
        success,
        failures,
    )


def setup_job_status(jobid, count_asteroids):
    update_progress_status(
        jobid,
        step=1,
        status=2,
        count=count_asteroids,
        current=0,
        success=0,
        failures=0,
        t0=datetime.now(tz=timezone.utc),
    )

    update_progress_status(
        jobid,
        step=2,
        status=2,
        count=count_asteroids,
        current=0,
        success=0,
        failures=0,
        t0=datetime.now(tz=timezone.utc),
    )


def run_job(jobid: int):
    print("run job: %s" % jobid)

    dao = PredictOccultationJobDao()

    job = dao.get_job_by_id(jobid)

    config = get_configs()

    # Cria um diretório para o job    
    job_path = get_job_path(job['id'])

    # Escreve o arquivo job.json
    make_job_json_file(job, job_path)

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


def submit_tasks(jobid: int):
    print("submit_tasks")

    # Settings Parsl configurations
    envname = os.getenv('PARSL_ENV', 'linea')
    parsl_conf = get_config(envname)
    parsl.clear()
    parsl.load(parsl_conf)

    # Read Inputs from job.json
    job = read_job_json_by_id(jobid)

    # Paths de execução
    original_path = os.getcwd()
    current_path = pathlib.Path(job.get("path"))

    DEBUG = job.get("debug", False)

    # Create a Log file
    logname = "submit_tasks"
    log = get_logger(current_path, f"{logname}.log", DEBUG)

    current_idx = 0
    step1_success, step1_failures = 0, 0
    step1_count = 0

    step2_current_idx = 0
    step2_success, step2_failures = 0, 0
    step2_count = 0

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
                "check_tasks": None,
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

        job.update(
            {
                "predict_start_date": str(PREDICT_START.date()),
                "predict_end_date": str(PREDICT_END.date()),
            }
        )

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
            raise (
                "No asteroid satisfying the criteria %s and %s. There is nothing to run."
                % (job["filter_type"], job["filter_value"])
            )

        # Lista de Jobs do Condor.
        # htc_jobs = list()

        # Diretório para armazenar os jobs que foram submetidos no HTCondor.
        # Cada job vai gerar um arquivo neste diretório
        # Que depois vai ser verificado pela segunda etapa.
        # Esses arquivos é que fazem a ligação entre as 2 etapas do pipeline.
        HTC_JOBS_PATH = current_path.joinpath("jobs")
        HTC_JOBS_PATH.mkdir(parents=True, exist_ok=False)

        JOBS_CALLBACK_PATH = current_path.joinpath("callback")
        JOBS_CALLBACK_PATH.mkdir(parents=True, exist_ok=False)

        hb_t0 = datetime.now(tz=timezone.utc)

        jobs_asteroids = list()
        workdir = os.getenv("PIPELINE_PATH")

        step1_count = len(asteroids)

        for asteroid in asteroids:
            log.info(
                "---------------< Running: %s / %s >---------------"
                % (current_idx + 1, step1_count)
            )
            log.info("Asteroid: [%s]" % asteroid["name"])

            is_abort = check_abort_job(jobid)
            if is_abort:
                raise AbortError("Job ID %s aborted!" % str(jobid), -1)

            a = Asteroid(
                id=asteroid["id"],
                name=asteroid["name"],
                number=asteroid["number"],
                base_dynclass=asteroid["base_dynclass"],
                dynclass=asteroid["dynclass"],
                base_path=ASTEROID_PATH,
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
            # have_des_obs = True

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
                step1_failures += 1
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
                step1_failures += 1
                # Ignora as proximas etapas para este asteroid.
                continue

            # Observations --------------------------------------------------
            have_obs = a.check_observations(days_to_expire=OBSERVATIONS_DAYS_TO_EXPIRE)

            if have_obs is False:
                log.warning(
                    "Asteroid [%s] Ignored for not having Observations."
                    % asteroid["name"]
                )
                # TODO: guardar informações dos asteroids ignorados e os motivos.

                current_idx += 1
                step1_failures += 1
                # Ignora as proximas etapas para este asteroid.
                continue

            step1_success += 1
            current_idx += 1

            update_progress_status(
                jobid,
                step=1,
                status=2,
                count=len(asteroids),
                current=current_idx,
                success=step1_success,
                failures=step1_failures,
                t0=hb_t0,
            )

            # ======================= Submeter o Job por asteroide ==========================
            log.info("Submitting the Job. [%s]" % str(a.get_path()))

            start_date = str(PREDICT_START.date())
            end_date = str(PREDICT_END.date())
            name = a.alias
            number = a.number
            path = str(a.get_path())

            try:
                proc = run_pipeline(
                    (name, start_date, end_date, number, path, workdir),
                    stderr=f"{path}/{name}.err",
                    stdout=f"{path}/{name}.out",
                )
                asteroid["job"] = proc
                asteroid["done"] = False
                jobs_asteroids.append(asteroid)

            except Exception:
                step2_failures = +1
                log.error("Error running asteroid %s" % name)
                continue

        update_progress_status(
            jobid,
            step=1,
            status=3,
            count=len(asteroids),
            current=current_idx,
            success=step1_success,
            failures=step1_failures,
            t0=hb_t0,
        )

        log.info("All jobs have been submitted.")

        update_progress_status(
            jobid,
            step=2,
            status=2,
            count=len(jobs_asteroids),
            current=step2_current_idx,
            success=step2_success,
            failures=step2_failures,
            t0=hb_t0,
        )

        log.info("Jobs to Parsl: %i" % len(jobs_asteroids))

        # # Monitoramento parcial das tasks
        is_done = list()
        step2_count = len(jobs_asteroids)

        while is_done.count(True) != step2_count:
            is_done = list()
            is_abort = check_abort_job(jobid)
            # log.debug("IS ABORT: %s" % str(is_abort))
            for proc in jobs_asteroids:
                task = proc.get("job")
                proc_is_done = task.done()

                if is_abort and not proc_is_done:
                    task.set_exception(
                        AbortError("Asteroid %s aborted!" % str(proc["name"]), -1)
                    )

                if not proc.get("done", False) and proc_is_done:
                    proc["done"] = proc_is_done
                    errobj = task.exception()

                    if errobj:
                        try:
                            status = errobj.exitcode
                        except AttributeError:
                            status = -1
                        log.warn('JobParslError: %s' % str(status))
                    else:
                        status = task.result()

                    ast_obj = Asteroid(name=proc["name"], base_path=ASTEROID_PATH)
                    ast_obj.set_log(log)

                    step2_current_idx += 1

                    if status:
                        step2_failures += 1
                        log.warn(
                            "Asteroid [%s] - Bash exit with code %s"
                            % (proc["name"], str(status))
                        )
                    else:
                        step2_success += 1
                        start_date = str(PREDICT_START.date())
                        end_date = str(PREDICT_END.date())

                        ingested_occ_count = ast_obj.register_occultations(
                            start_date, end_date, jobid
                        )

                        update_progress_status(
                            jobid,
                            step=2,
                            status=2,
                            count=len(jobs_asteroids),
                            current=step2_current_idx,
                            success=step2_success,
                            failures=step2_failures,
                            t0=hb_t0,
                        )

                        log.info(
                            "Asteroid: [%s] Occultations: [%s]"
                            % (ast_obj.name, str(ingested_occ_count))
                        )

                is_done.append(proc.get("done"))
            log.info(
                "Predict Occultation Parsl Task Running: %s/%s"
                % (is_done.count(True), len(jobs_asteroids))
            )
            # log.debug("N# FAILED: %i" % step2_failures)
            # log.debug("N# SUCCESSED: %i" % step2_success)
            time.sleep(30)

        is_abort = check_abort_job(jobid)
        if is_abort:
            raise AbortError("Job ID %s aborted!" % str(jobid), -1)

        update_progress_status(
            jobid,
            step=2,
            status=3,
            count=len(jobs_asteroids),
            current=step2_current_idx,
            success=step2_success,
            failures=step2_failures,
            t0=hb_t0,
        )

        job.update({"status": "Completed"})
    except AbortError as e:
        trace = traceback.format_exc()
        log.error(trace)
        log.error("ABORT ERROR: %s" % e)

        # Status 4 = Failed
        job.update(
            {
                "status": "Aborted",
                "error": str(e),
                "traceback": str(trace),
            }
        )

        update_progress_status(
            jobid,
            step=1,
            status=5,
            count=step1_count,
            current=current_idx,
            success=step1_success,
            failures=step1_failures,
            t0=hb_t0,
        )

        update_progress_status(
            jobid,
            step=2,
            status=5,
            count=step2_count,
            current=step2_current_idx,
            success=step2_success,
            failures=step2_failures,
            t0=hb_t0,
        )

    except Exception as e:
        trace = traceback.format_exc()
        log.error(trace)
        log.error(e)

        # Status 4 = Failed
        job.update(
            {
                "status": "Failed",
                "error": str(e),
                "traceback": str(trace),
            }
        )

        update_progress_status(
            jobid,
            step=1,
            status=4,
            count=step1_count,
            current=current_idx,
            success=step1_success,
            failures=step1_failures,
            t0=hb_t0,
        )

        update_progress_status(
            jobid,
            step=2,
            status=4,
            count=step2_count,
            current=step2_current_idx,
            success=step2_success,
            failures=step2_failures,
            t0=hb_t0,
        )
    finally:
        l_consolidated = list()

        asteroids = retrieve_asteroids(job["filter_type"], job["filter_value"])
        consolid_current_idx = 1

        for asteroid in asteroids:
            log.info(
                "---------------< Consolidated: %s / %s >---------------"
                % (consolid_current_idx, job["count_asteroids"])
            )
            log.info("Asteroid: [%s]" % asteroid["name"])

            current_path = pathlib.Path(job.get("path"))
            ASTEROID_PATH = current_path.joinpath("asteroids")

            ast_obj = Asteroid(name=asteroid["name"], base_path=ASTEROID_PATH)
            ast_obj.set_log(log)

            consolid_current_idx += 1

            consolidated = ast_obj.consiladate()

            l_consolidated.append(consolidated)

        log.debug("Job completed - Update Progress bar step2")

        job.update({"submited_all_jobs": True, "condor_job_submited": len(asteroids)})
        update_job(job)

        # ========================= Consolidando resultados ============================
        if len(l_consolidated) > 0:
            print("l_consolidate > 0")
            consolidate_job_results(l_consolidated, current_path)

        log.info("Ingest Predict Occultation Job Results in database")
        count_results_ingested = ingest_job_results(current_path, jobid)
        log.debug(
            "Predict Occultation Job Results ingested: %s" % count_results_ingested
        )

        complete_job(job, log, job.get("status", "Completed"))

        log.info("Finish Submit tasks")
        os.chdir(original_path)
        parsl.clear()
        return True


def consolidate_job_results(consolidated, job_path):
    print("Consolidating Job Results.")
    # log.info("Consolidating Job Results.")
    df_result = pd.DataFrame(
        consolidated,
        columns=[
            "ast_id",
            "name",
            "number",
            "base_dynclass",
            "dynclass",
            "des_obs",
            "des_obs_start",
            "des_obs_finish",
            "des_obs_exec_time",
            "des_obs_gen_run",
            "des_obs_tp_start",
            "des_obs_tp_finish",
            "bsp_jpl_start",
            "bsp_jpl_finish",
            "bsp_jpl_dw_time",
            "bsp_jpl_dw_run",
            "bsp_jpl_tp_start",
            "bsp_jpl_tp_finish",
            "obs_source",
            "obs_start",
            "obs_finish",
            "obs_dw_time",
            "obs_dw_run",
            "obs_tp_start",
            "obs_tp_finish",
            "orb_ele_source",
            "orb_ele_start",
            "orb_ele_finish",
            "orb_ele_dw_time",
            "orb_ele_dw_run",
            "orb_ele_tp_start",
            "orb_ele_tp_finish",
            "ref_orb_start",
            "ref_orb_finish",
            "ref_orb_exec_time",
            "pre_occ_count",
            "pre_occ_start",
            "pre_occ_finish",
            "pre_occ_exec_time",
            "ing_occ_count",
            "ing_occ_start",
            "ing_occ_finish",
            "ing_occ_exec_time",
            "exec_time",
            "messages",
            "status",
        ],
    )

    result_filepath = pathlib.Path(job_path, "job_consolidated.csv")
    mode = "a"
    header = False

    if not result_filepath.exists():
        mode = "w"
        header = True
    df_result.to_csv(
        result_filepath,
        encoding="utf-8",
        sep=";",
        index=False,
        mode=mode,
        header=header,
    )
    del df_result
    # log.info("File with the consolidated Job data. [%s]" % result_filepath)


def complete_job(job, log, status):
    consolidated_filepath = pathlib.Path(job.get("path"), "job_consolidated.csv")
    df = pd.read_csv(consolidated_filepath, delimiter=";")

    l_status = df["status"].to_list()
    count_success = int(l_status.count(1))
    count_failures = int(l_status.count(2))
    occultations = int(df["ing_occ_count"].sum())
    ast_with_occ = int((df["ing_occ_count"] != 0).sum())

    log.info("AST WITH OCC: %i" % ast_with_occ)

    log.info(
        f"Count Success {count_success} Failure {count_failures} Occ {occultations} ast {ast_with_occ}"
    )

    t0 = datetime.fromisoformat(job.get("start"))
    t1 = datetime.now(tz=timezone.utc)
    tdelta = t1 - t0

    # Calc average time by asteroid
    avg_exec_time_asteroid = 0
    if job.get("count_asteroids") > 0:
        avg_exec_time_asteroid = int(
            tdelta.total_seconds() / job.get("count_asteroids")
        )

    # Status 3 = Completed
    job.update(
        {
            "status": status,
            "ast_with_occ": ast_with_occ,
            "occultations": occultations,
            "count_success": count_success,
            "count_failures": count_failures,
            "end": t1.isoformat(),
            "exec_time": tdelta.total_seconds(),
            "h_exec_time": humanize.naturaldelta(tdelta),
            "avg_exec_time": avg_exec_time_asteroid,
        }
    )

    log.info("Update Job status.")
    # write_job_file(current_path, job)
    update_job(job)

    # Remove o diretório de asteroids do job.
    if not job["debug"]:
        log.debug("Removing asteroid directory.")
        asteroid_path = pathlib.Path(job["path"]).joinpath("asteroids")
        shutil.rmtree(asteroid_path)
        log.info("Directory of asteroids has been removed!")

    log.info(
        "Asteroids Success: [%s] Failure: [%s] Total: [%s]"
        % (job["count_success"], job["count_failures"], job["count_asteroids"])
    )

    log.info("Execution Time: %s" % tdelta)
    log.info("Predict Occultation is done!.")


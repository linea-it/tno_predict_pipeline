# -*- coding: utf-8 -*-
import argparse
import configparser
import json
import os
import pathlib
import time
import traceback
import csv
from datetime import datetime, timezone

import humanize
import parsl
import sys
from asteroid import Asteroid
from library import (
    get_logger,
    read_inputs,
    retrieve_asteroids,
    write_job_file,
    ingest_observations,
    write_json,
    get_configs
)
from orbit_trace_apps import observed_positions, theoretical_positions
from parsl_config import htex_config
from dao import OrbitTraceJobDao, OrbitTraceJobResultDao
import pandas as pd
from io import StringIO
from pathlib import Path
import shutil
import subprocess
import uuid

def update_job(job) -> None:
    otjdao = OrbitTraceJobDao()
    otjdao.update_job(job)

    write_job_file(job.get('path'), job)

def orbit_trace_job_to_run():
    """Retorna o job com status=1 Idle mas antigo.

    Returns:
        job: Orbit Trace Job model
    """
    otjdao = OrbitTraceJobDao()
    job = otjdao.get_job_by_status(1)

    return job


def orbit_trace_has_job_running() -> bool:
    """Verifica se há algum job com status = 2 Running.

    Returns:
        bool: True caso haja algum job sendo executado.
    """
    otjdao = OrbitTraceJobDao()
    job = otjdao.get_job_by_status(2)

    if job is not None:
        return True
    else:
        return False

def orbit_trace_job_queue():

    # Verifica se ha algum job sendo executado.
    if orbit_trace_has_job_running():
        # print("Já existe um job em execução.")
        return

    # Verifica o proximo job com status Idle
    job_to_run = orbit_trace_job_to_run()
    if not job_to_run:
        return

    # Inicia o job.
    # print("Deveria executar o job com ID: %s" % job_to_run.get("id")) 
    # orbit_trace_run_job(job_to_run.get("id"))
    return job_to_run.get("id")


def orbit_trace_make_job_json_file(job, path):

    job_data = dict({
        "id": job.get('id'),
        "status": "Submited",
        "submit_time": job.get('submit_time').astimezone(timezone.utc).isoformat(),
        "estimated_execution_time": str(job.get('estimated_execution_time')),
        "path": str(path),
        "match_radius": job.get('match_radius'),
        "filter_type": job.get('filter_type'),
        "filter_value": job.get('filter_value'),
        "bsp_days_to_expire": job.get('bps_days_to_expire', 0),
        "parsl_init_blocks": job.get('parsl_init_blocks', 600),        
        "debug": bool(job.get('debug', False)),
        "traceback": None,
        "error": None,
        "time_profile": [],     
        # TODO: Estes parametros devem ser gerados pelo pipeline lendo do config.
        # TODO: Bsp e leap second deve fazer a query e verificar o arquivo ou fazer o download.
        "period": ["2012-11-01", "2019-02-01"],
        "observatory_location": [289.193583333, -30.16958333, 2202.7],
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
    })

    write_job_file(path, job_data)

def run_job(jobid: int):
    otjdao = OrbitTraceJobDao()    

    # TODO: ONLY DEVELOPMENT
    # otjdao.development_reset_job(jobid)

    job = otjdao.get_job_by_id(jobid)

    config = get_configs()
    orbit_trace_root = config["DEFAULT"].get("OrbitTraceJobPath")

    # Cria um diretório para o job
    # TODO: ONLY DEVELOPMENT
    # folder_name = f"teste_{job['id']}"
    # folder_name = f"orbit_trace_{job['id']}-{str(uuid.uuid4())[:8]}"
    folder_name = f"{job['id']}-{str(uuid.uuid4())[:8]}"        
    job_path = Path(orbit_trace_root).joinpath(folder_name)
    if job_path.exists():
        shutil.rmtree(job_path)
    job_path.mkdir(parents=True, exist_ok=False)

    # Escreve o arquivo job.json
    orbit_trace_make_job_json_file(job, job_path)

    # Executa o job usando subproccess.
    env_file = Path(os.environ['EXECUTION_PATH']).joinpath('env.sh')
    proc = subprocess.Popen(
        # f"source /lustre/t1/tmp/tno/pipelines/env.sh; python orbit_trace.py {job_path}",
        f"source {env_file}; python orbit_trace.py {job_path}",
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


def ingest_job_results(job_path, job_id):

    file_names = ['asteroids_failed.json', 'asteroids_success.json']

    data = list()

    for fname in file_names:
        filepath = Path(job_path).joinpath(fname)
        if filepath.exists():
            asteroids = read_inputs(job_path, fname)
            for asteroid in asteroids:
                status = 2
                if fname == 'asteroids_success.json':
                    status = 1
             
                row = dict({
                    'name': asteroid['name'],
                    'number': asteroid.get('number', None),           
                    'base_dynclass': asteroid['base_dynclass'],
                    'dynclass': asteroid['dynclass'],       
                    'status': status,
                    'spk_id': asteroid.get('spkid', None),
                    'observations': asteroid.get('observations_count', 0),
                    'ccds': len(asteroid.get('ccds', [])),           
                    'error': asteroid.get('error', None),
                    'asteroid_id': asteroid['id'],                    
                    'job_id': job_id,                    
                }) 
                data.append(row)
            
    df_results = pd.DataFrame(
        data,
        columns=[
                "name",
                "number",
                "base_dynclass",
                "dynclass",
                "status",
                "spk_id",
                "observations",
                "ccds",                
                "error",
                "asteroid_id",                
                "job_id",                
            ],
        )

    # Guarda uma copia das observações no diretório do Asteroid.
    filepath = Path(job_path, "orbit_trace_results.csv")
    df_results.to_csv(filepath, sep=";", header=True, index=False)

    str_data = StringIO()
    df_results.to_csv(
        str_data, sep="|", header=True, index=False,
    )
    str_data.seek(0)

    otjrdao = OrbitTraceJobResultDao()
    otjrdao.delete_by_job_id(job_id)
    rowcount = otjrdao.import_orbit_trace_results(str_data)

    return rowcount

def main(path):
    try:
        # Carrega as variaveis de configuração do arquivo config.ini
        config = configparser.ConfigParser()
        config.read("config.ini")

        # Paths de execução
        original_path = os.getcwd()
        # os.environ["EXECUTION_PATH"] = original_path

        current_path = path

        # Altera o path de execução
        # A raiz agora é o path passado como parametro.
        os.chdir(current_path)

        # Read Inputs from job.json
        job = read_inputs(current_path, "job.json")

        # Job ID
        jobid = int(job.get("id"))

        DEBUG = job.get("debug", False)

        # Start Running Time
        t0 = datetime.now(tz=timezone.utc)

        # Create a Log file
        log = get_logger(current_path, "orbit_trace.log", DEBUG)
        log.info("--------------< DES Object Identifier >--------------")
        log.info("Job ID: [%s]" % jobid)
        log.info("Current Path: [%s]" % current_path)
        log.info("DEBUG: [%s]" % DEBUG)

        job.update(
            {
                "status": "Running",
                "start": t0.isoformat(),
                "end": None,
                "exec_time": 0,
                "count_success": 0,
                "count_failures": 0,
                "time_profile": [],
            }
        )

        log.info("Update Job status to running.")
        # write_job_file(current_path, job)
        update_job(job)
    # try:
        # log.debug('Job Inputs: %s' % json.dumps(job))

        # Setting Inputs
        DES_CATALOGS_BASEPATH = config["DEFAULT"].get("DesCatalogPath")
        log.info("DES_CATALOGS_BASEPATH: [%s]" % DES_CATALOGS_BASEPATH)

        BSP_PLANETARY = job["bsp_planetary"]["absolute_path"]
        log.info("BSP_PLANETARY: [%s]" % BSP_PLANETARY)

        LEAP_SECOND = job["leap_seconds"]["absolute_path"]
        log.info("LEAP_SECOND: [%s]" % LEAP_SECOND)

        des_period = job.get("period", ["2012-11-01", "2019-02-01"])
        DES_START_PERIOD = des_period[0]
        log.info("DES_START_PERIOD: [%s]" % DES_START_PERIOD)
        DES_FINISH_PERIOD = des_period[1]
        log.info("DES_FINISH_PERIOD: [%s]" % DES_FINISH_PERIOD)

        # Location of observatory: [longitude, latitude, elevation]
        OBSERVATORY_LOCATION = job.get(
            "observatory_location", [289.193583333, -30.16958333, 2202.7]
        )
        log.info("OBSERVATORY_LOCATION: %s" % OBSERVATORY_LOCATION)

        MATCH_RADIUS = job.get("match_radius", 2)
        log.info("MATCH_RADIUS: [%s]" % MATCH_RADIUS)

        BSP_DAYS_TO_EXPIRE = job.get("bsp_days_to_expire", 60)

        # =========================== Asteroids ===========================

        # Retrieve Asteroids.
        log.info("Retriving Asteroids started")

        step_t0 = datetime.now(tz=timezone.utc)

        # Lista com os asteroids que falharem durante a execução.
        a_failed = list()

        asteroids = retrieve_asteroids(job["filter_type"], job["filter_value"])

        #asteroids = asteroids[0:20]

        job.update({"count_asteroids": len(asteroids)})

        step_t1 = datetime.now(tz=timezone.utc)
        step_tdelta = step_t1 - step_t0

        # Update Job time profile
        job["time_profile"].append(
            dict(
                {
                    "step": "retrieve_asteroids",
                    "start": step_t0.isoformat(),
                    "end": step_t1.isoformat(),
                    "exec_time": step_tdelta.total_seconds(),
                }
            )
        )

        log.info("Asteroids Count: %s" % job["count_asteroids"])

        log.info(
            "Retriving Asteroids Finished in %s"
            % humanize.naturaldelta(step_tdelta, minimum_unit="microseconds")
        )

        # Update Job File
        # write_job_file(current_path, job)
        update_job(job)

        if job["count_asteroids"] == 0:
            raise ("No asteroid satisfying the criteria %s and %s. There is nothing to run." % (job["filter_type"], job["filter_value"]))

        # =========================== CCDs ===========================
        # Retrieve CCDs
        log.info("Retriving CCDs started")

        step_t0 = datetime.now(tz=timezone.utc)

        count_ccds = 0
        i = 0
        success_asteroids = list()
        ccd_ast_failed = 0
        all_ccds = list()
        for asteroid in asteroids:

            if asteroid["status"] != "failure":

                a = Asteroid(
                    id=asteroid["id"],
                    name=asteroid["name"],
                    number=asteroid["number"],
                    base_dynclass=asteroid["base_dynclass"],
                    dynclass=asteroid["dynclass"],
                )

                a.set_log("orbit_trace")

                ccds = a.retrieve_ccds(LEAP_SECOND)
                count_ccds += len(ccds)
                all_ccds += ccds

                if len(ccds) == 0:
                    asteroid.update(
                        {
                            "status": "failure",
                            "ccds": [],
                            "error": "Failed during retrieve ccds step because no ccd was found for this asteroid.",
                        }
                    )
                    ccd_ast_failed += 1

                    a_failed.append(asteroid)
                else:
                    asteroid.update({"ccds": ccds, "path": str(a.path)})

                    success_asteroids.append(asteroid)

                log.info(
                    "Asteroid: [%s] CCDs: [%s]" % (asteroid["name"], len(asteroid["ccds"]))
                )

                del a

            i += 1

            log.debug("Query CCDs: %s/%s" % (i, len(asteroids)))

        asteroids = success_asteroids

        step_t1 = datetime.now(tz=timezone.utc)
        step_tdelta = step_t1 - step_t0

        # Update Job time profile
        job["time_profile"].append(
            dict(
                {
                    "step": "retrieve_ccds",
                    "start": step_t0.isoformat(),
                    "end": step_t1.isoformat(),
                    "exec_time": step_tdelta.total_seconds(),
                    "ast_success": len(asteroids),
                    "ast_failed": ccd_ast_failed,
                }
            )
        )

        job.update({"count_ccds": count_ccds})
        log.info("CCDs Count: %s" % count_ccds)

        log.info(
            "Retriving CCDs Finished in %s. Asteroids Success [%s] Failed [%s]"
            % (
                humanize.naturaldelta(step_tdelta, minimum_unit="microseconds"),
                len(asteroids),
                ccd_ast_failed,
            )
        )

        # Update Job File
        # write_job_file(current_path, job)
        update_job(job)

        # Escreve um csv com todos os ccds utilizados no job
        ccds_csv_filename = pathlib.Path(current_path, "ccds.csv")
        with open(ccds_csv_filename, "w") as csvfile:
            fieldnames = ["id", "date_obs", "exptime", "path", "filename"]
            writer = csv.DictWriter(
                csvfile, fieldnames=fieldnames, delimiter=";", extrasaction="ignore"
            )

            writer.writeheader()
            writer.writerows(all_ccds)


        # =========================== BSP ===========================
        # Retrieve BSPs
        # Etapa sequencial
        log.info("Retriving BSP JPL started")

        step_t0 = datetime.now(tz=timezone.utc)

        i = 0
        success_asteroids = list()
        bsp_ast_failed = 0
        for asteroid in asteroids:

            if asteroid["status"] != "failure":

                a = Asteroid(
                    id=asteroid["id"],
                    name=asteroid["name"],
                    number=asteroid["number"],
                    base_dynclass=asteroid["base_dynclass"],
                    dynclass=asteroid["dynclass"],
                )

                a.set_log("orbit_trace")

                have_bsp_jpl = a.check_bsp_jpl(
                    start_period=DES_START_PERIOD,
                    end_period=DES_FINISH_PERIOD,
                    days_to_expire=BSP_DAYS_TO_EXPIRE,
                )

                if have_bsp_jpl and a.bsp_jpl["filename"] is not None:
                    # Recupera o Path para o BSP
                    bsp_path = a.get_bsp_path()

                    asteroid.update({"bsp_path": str(bsp_path)})

                    # Recupera o SPKID que será usado na proxima etapa.
                    spkid = a.get_spkid()

                    if spkid is None:
                        asteroid.update(
                            {
                                "status": "failure",
                                "error": "It failed during the retrieve bsp jpl step because it could not identify the SPKID.",
                            }
                        )
                        bsp_ast_failed += 1
                        a_failed.append(asteroid)

                    else:
                        asteroid.update({"spkid": spkid})
                        success_asteroids.append(asteroid)
                else:
                    msg = "Failed during retrieve bsp from jpl step. "
                    if a.bsp_jpl is not None and a.bsp_jpl["message"]:
                        msg += a.bsp_jpl["message"]

                    asteroid.update({"status": "failure", "error": msg})

                    bsp_ast_failed += 1
                    a_failed.append(asteroid)

            i += 1

            log.debug("BSPs JPL: %s/%s" % (i, len(asteroids)))

        asteroids = success_asteroids

        step_t1 = datetime.now(tz=timezone.utc)
        step_tdelta = step_t1 - step_t0

        # Update Job time profile
        job["time_profile"].append(
            dict(
                {
                    "step": "retrieve_bsp",
                    "start": step_t0.isoformat(),
                    "end": step_t1.isoformat(),
                    "exec_time": step_tdelta.total_seconds(),
                    "ast_success": len(asteroids),
                    "ast_failed": bsp_ast_failed,
                }
            )
        )

        log.info(
            "Retriving BSP JPL Finished %s. Asteroids Success [%s] Failed [%s]"
            % (
                humanize.naturaldelta(step_tdelta, minimum_unit="microseconds"),
                len(asteroids),
                bsp_ast_failed,
            )
        )

        # Update Job File
        # write_job_file(current_path, job)
        update_job(job)

        # =========================== Parsl ===========================
        log.info("Parsl Load started")

        step_t0 = datetime.now(tz=timezone.utc)

        # Load Parsl Configs
        parsl.clear()

        htex_config.run_dir = os.path.join(current_path, "runinfo")

        # Verifica se a configuração tem a label htcondor
        try:
            # Htcondor config with full nodes
            htex_config.executors[0].provider.channel.script_dir = os.path.join(
                current_path, "script_dir"
            )

            # Adicionar o ID do processo ao arquivo de submissão do condor
            htex_config.executors[0].provider.scheduler_options += "+AppId = {}\n".format(
                jobid
            )

            # TODO: Este parametro pode vir do config.ini
            MAX_PARSL_BLOCKS = 400
            # Alterar a quantidade de CPUs reservadas
            # de acordo com a quantidade de ccds a serem processados divididos por 2
            blocks = (job["count_ccds"] // 2) + 1
            if blocks < MAX_PARSL_BLOCKS:
                # Mesmo que a etapa Observed Positions seja paralelizada
                # por ccds que é uma quantidade muito superior que a de asteroids
                # estou preferindo limitar os cores em paralelos a metade do necessário.
                # Desta forma evito que o cluster seja todo utilizado por este processo.
                # e diminui o tempo de espera do Parsl Load para jobs menores.
                #
                # Se a quantidade de blocks for maior que o MAX_PARSL_BLOCKS,
                # será utilizado todos os blocos definidos no parsl_config init_blocks
                htex_config.executors[0].provider.init_blocks = int(blocks)
                log.info("Parsl limiting the amount of Blocks to [%s]" % blocks)

            job.update({"parsl_init_blocks": htex_config.executors[0].provider.init_blocks})

        except:
            # Considera que é uma execução local
            pass

        parsl.load(htex_config)

        step_t1 = datetime.now(tz=timezone.utc)
        step_tdelta = step_t1 - step_t0

        # Update Job time profile
        job["time_profile"].append(
            dict(
                {
                    "step": "parsl_load",
                    "start": step_t0.isoformat(),
                    "end": step_t1.isoformat(),
                    "exec_time": step_tdelta.total_seconds(),
                }
            )
        )

        log.info(
            "Parsl Load finished in %s."
            % (humanize.naturaldelta(step_tdelta, minimum_unit="microseconds"))
        )

        # =========================== Theoretical ===========================
        # Calculando as posições teoricas
        log.info("Calculating theoretical positions started")

        step_t0 = datetime.now(tz=timezone.utc)

        futures = list()
        for asteroid in asteroids:
            if asteroid["status"] != "failure":
                futures.append(
                    theoretical_positions(
                        asteroid, BSP_PLANETARY, LEAP_SECOND, OBSERVATORY_LOCATION
                    )
                )

        # Monitoramento parcial das tasks
        is_done = list()
        while is_done.count(True) != len(futures):
            is_done = list()
            for f in futures:
                is_done.append(f.done())
            log.debug(
                "Theoretical Positions running: %s/%s" % (is_done.count(True), len(futures))
            )
            time.sleep(30)

        asteroids = list()
        theo_ast_failed = 0
        for task in futures:
            asteroid = task.result()
            if asteroid["status"] == "failure":
                a_failed.append(asteroid)
                theo_ast_failed += 1
                log.warning("Asteroid [%s] %s" % (asteroid["name"], asteroid["error"]))
            else:
                asteroids.append(asteroid)

        log.info(
            "Calculating theoretical positions Finished %s. Asteroids Success [%s] Failed [%s]"
            % (
                humanize.naturaldelta(step_tdelta, minimum_unit="microseconds"),
                len(asteroids),
                theo_ast_failed,
            )
        )

        step_t1 = datetime.now(tz=timezone.utc)
        step_tdelta = step_t1 - step_t0

        # Update Job time profile
        job["time_profile"].append(
            dict(
                {
                    "step": "theoretical_positions",
                    "start": step_t0.isoformat(),
                    "end": step_t1.isoformat(),
                    "exec_time": step_tdelta.total_seconds(),
                    "ast_success": len(asteroids),
                    "ast_failed": theo_ast_failed,
                }
            )
        )

        # Update Job File
        # write_job_file(current_path, job)
        update_job(job)

        # =========================== Observed ===========================
        # Calculando as posições Observadas
        log.info("Calculating observed positions started")

        step_t0 = datetime.now(tz=timezone.utc)

        futures = list()
        htcondor_job_submited = 0
        htcondor_jobs_completed = 0
        htcondor_jobs_removed = 0
        for asteroid in asteroids:
            idx = 0
            for ccd in asteroid["ccds"]:
                if ccd["theoretical_coordinates"] is not None:
                    # Monta o path para os catalogos
                    ccd["path"] = os.path.join(DES_CATALOGS_BASEPATH, ccd["path"])

                    futures.append(
                        observed_positions(
                            idx=idx,
                            name=asteroid["name"],
                            asteroid_id=asteroid["id"],
                            ccd=ccd,
                            asteroid_path=asteroid["path"],
                            radius=MATCH_RADIUS,
                        )
                    )
                    idx += 1
                    htcondor_job_submited += 1
                    log.debug(
                        "Submited: Asteroid [%s] CCD: [%s] IDX:[%s]"
                        % (asteroid["name"], ccd["id"], idx)
                    )

        log.debug("All Obeserved Positions Jobs are Submited")

        # Monitoramento parcial das tasks
        is_done = list()
        while is_done.count(True) != len(futures):
            is_done = list()
            for f in futures:
                is_done.append(f.done())
            log.debug(
                "Observed Positions running: %s/%s" % (is_done.count(True), len(futures))
            )
            time.sleep(30)

        results = dict({})
        for task in futures:
            # TODO: Guardar o time profile, tempo gasto em cada observação.
            asteroid_name, ccd, obs_coordinates = task.result()

            alias = asteroid_name.replace(" ", "_")
            if alias not in results:
                results[alias] = dict({"ccds": list(), "observations": list()})

            results[alias]["ccds"].append(ccd)
            if obs_coordinates is not None:
                results[alias]["observations"].append(obs_coordinates)

            htcondor_jobs_completed += 1

        # TODO: Implementar remoção de jobs por time out.
        job.update({
            'condor_job_submited': htcondor_job_submited,
            'condor_job_completed': htcondor_jobs_completed,
            'condor_job_removed': htcondor_jobs_removed
        })
        update_job(job)

        # Agrupar os resultados.
        count_observations = 0
        obs_pos_ast_failed = 0
        for asteroid in asteroids:
            alias = asteroid["name"].replace(" ", "_")
            asteroid.update(
                {
                    "ccds": results[alias]["ccds"],
                    "observations": results[alias]["observations"],
                    "observations_count": len(results[alias]["observations"]),
                }
            )
            count_observations += asteroid["observations_count"]

            # Verificar se o asteroid teve algum erro
            # no Calculo das posições Observadas
            for ccd in asteroid["ccds"]:
                if "error" in ccd and ccd["error"] is not None:
                    # Houve error em pelo menos 1 CCD do Asteroid.
                    # Basta ter um erro nos ccds para considerar o Asteroid como falha.
                    asteroid.update({"status": "failure", "error": ccd["error"]})

                    log.warning("Asteroid [%s] %s" % (asteroid["name"], ccd["error"]))

                    # Adiciona o Asteroid na lista de falhas
                    a_failed.append(asteroid)

                    obs_pos_ast_failed += 1

                    # Interrompe o loop nos ccds.
                    break

        # Remove do array de asteroid os que falharam.
        success_asteroids = list()
        for asteroid in asteroids:
            if asteroid["status"] != "failure":
                success_asteroids.append(asteroid)
                if not DEBUG:
                    # Remover os logs do diretório em caso de sucesso.
                    for p in pathlib.Path(asteroid["path"]).glob("des_obs*.log"):
                        p.unlink()

        asteroids = success_asteroids

        job.update({"count_observations": count_observations})

        step_t1 = datetime.now(tz=timezone.utc)
        step_tdelta = step_t1 - step_t0

        # Update Job time profile
        job["time_profile"].append(
            dict(
                {
                    "step": "observed_positions",
                    "start": step_t0.isoformat(),
                    "end": step_t1.isoformat(),
                    "exec_time": step_tdelta.total_seconds(),
                    "ast_success": len(asteroids),
                    "ast_failed": obs_pos_ast_failed,
                }
            )
        )

        log.info("Observations Count: %s" % count_observations)
        log.info(
            "Calculating observed positions Finished %s. Asteroids Success [%s] Failed [%s]"
            % (
                humanize.naturaldelta(step_tdelta, minimum_unit="microseconds"),
                len(asteroids),
                obs_pos_ast_failed,
            )
        )

        # Update Job File
        # write_job_file(current_path, job)
        update_job(job)

        # TODO: Fim da utilização do Cluster, liberar os cores utilizados.
        parsl.clear()
        # parsl.HighThroughputExecutor.shutdown()

        # =========================== Ingest Observations ===========================
        # Ingere as posições observadas no banco de dados
        # ETAPA SEQUENCIAL!
        log.info("Ingest the observations into the database started")

        step_t0 = datetime.now(tz=timezone.utc)

        ingested_obs = 0
        ingested_ast_failed = 0
        success_asteroids = list()
        for asteroid in asteroids:

            alias = asteroid["name"].replace(" ", "_")

            observations = results[alias]["observations"]

            if len(observations) > 0:
                result = ingest_observations(asteroid["path"], observations)
                asteroid.update({"ot_ing_obs": result})

                if "error" in result:
                    # Adiciona o Asteroid na lista de falhas
                    a_failed.append(asteroid)
                    ingested_ast_failed += 1
                    log.warning("Asteroid [%s] %s" % (asteroid["name"], result["error"]))
                    asteroid.update({"status": "failed", "error": result["error"]})
                else:
                    success_asteroids.append(asteroid)
                    ingested_obs += result["count"]

        asteroids = success_asteroids

        step_t1 = datetime.now(tz=timezone.utc)
        step_tdelta = step_t1 - step_t0

        # Update Job time profile
        job["time_profile"].append(
            dict(
                {
                    "step": "ingest_observations",
                    "start": step_t0.isoformat(),
                    "end": step_t1.isoformat(),
                    "exec_time": step_tdelta.total_seconds(),
                    "ast_success": len(asteroids),
                    "ast_failed": ingested_ast_failed,
                }
            )
        )

        log.info("Observations Ingested: %s" % ingested_obs)
        log.info(
            "Ingest the observations into the database Finished %s. Asteroids Success [%s] Failed [%s]"
            % (
                humanize.naturaldelta(step_tdelta, minimum_unit="microseconds"),
                len(asteroids),
                ingested_ast_failed,
            )
        )

        # Update Job File
        # write_job_file(current_path, job)
        update_job(job)

        # =========================== Consolidate ===========================
        log.info("Write Asteroid Data in json started")

        step_t0 = datetime.now(tz=timezone.utc)

        for asteroid in asteroids:
            # asteroid.update({'status': 'completed'})
            a = Asteroid(
                id=asteroid["id"],
                name=asteroid["name"],
                number=asteroid["number"],
                base_dynclass=asteroid["base_dynclass"],
                dynclass=asteroid["dynclass"],
            )

            a.set_log("orbit_trace")

            # TODO: Faltou adicionar os time profiles da etapa Observed Positions.
            a.ot_theo_pos = asteroid["ot_theo_pos"]
            # a.ot_obs_pos = asteroid['ot_obs_pos']
            a.ot_ing_obs = asteroid["ot_ing_obs"]

            a.write_asteroid_json()

        job.update({"count_success": len(asteroids)})

        # Escreve um Json com os asteroids que falharam.
        job.update({"count_failures": 0})
        if len(a_failed) > 0:
            job.update({"count_failures": len(a_failed)})

            log.info("Write Asteroids with failed status in json")
            failed_json = pathlib.Path(current_path, "asteroids_failed.json")
            write_json(failed_json, a_failed)

        # TODO: Criar um heartbeat.

        # TODO: Apenas para Debug
        # Não é necessário, remover no futuro
        log.info("Write Asteroids with success status in json")
        success_json = pathlib.Path(current_path, "asteroids_success.json")
        write_json(success_json, asteroids)

        log.info("Ingest Orbit Trace Job Results in database")
        count_results_ingested = ingest_job_results(current_path, jobid)
        log.debug("Orbit Trace Job Results ingested: %s" % count_results_ingested)

        step_t1 = datetime.now(tz=timezone.utc)
        step_tdelta = step_t1 - step_t0

        # Update Job time profile
        job["time_profile"].append(
            dict(
                {
                    "step": "consolidate",
                    "start": step_t0.isoformat(),
                    "end": step_t1.isoformat(),
                    "exec_time": step_tdelta.total_seconds(),
                }
            )
        )

        log.info(
            "Consolidate Finish in %s"
            % humanize.naturaldelta(step_tdelta, minimum_unit="microseconds")
        )

        # Status 3 = Completed
        job.update({"status": "Completed"})


    except Exception as e:
        trace = traceback.format_exc()
        log.error(trace)
        log.error(e)

        # Status 4 = Failed
        job.update(
            {"status": "Failed", "error": str(e), "traceback": str(trace),}
        )

    finally:
        parsl.clear()

        t1 = datetime.now(tz=timezone.utc)
        tdelta = t1 - t0

        # Calc average time by asteroid
        avg_exec_time_asteroid = 0
        if (job.get('count_asteroids') > 0):
            avg_exec_time_asteroid = int(tdelta.total_seconds() / job.get('count_asteroids'))
        
        # Calc average time by CCDs
        avg_exec_time_ccd = 0
        if (job.get('count_ccds') > 0):
            avg_exec_time_ccd = int(tdelta.total_seconds() / job.get('count_ccds'))

        job.update(
            {
                "end": t1.isoformat(),
                "exec_time": tdelta.total_seconds(),
                "h_exec_time": humanize.naturaldelta(tdelta),
                "avg_exec_time_asteroid": avg_exec_time_asteroid,
                "avg_exec_time_ccd": avg_exec_time_ccd
            }
        )

        log.info("Update Job status.")
        # write_job_file(current_path, job)
        update_job(job)

        # Altera o path de execução para o path original
        os.chdir(original_path)

        log.info(
            "Asteroids Success: [%s] Failure: [%s]"
            % (job["count_success"], job["count_failures"])
        )

        log.info("Execution Time: %s" % tdelta)
        log.info("Identification of DES object is done!.")

# Como Utilízar:
# cd /archive/des/tno/dev/nima/pipeline
# source env.sh
# python orbit_trace.py 1 /archive/des/tno/dev/nima/pipeline/examples/orbit_trace_job

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("path", help="Job Path")
    args = parser.parse_args()

    sys.exit(main(args.path))

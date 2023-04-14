#!/usr/bin/env python3
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

from library import (
    get_logger,
    read_inputs,
    write_job_file,
    retrieve_asteroids,
    submit_job,
)

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
        logname = "predict_occ"
        log = get_logger(current_path, "%s.log" % logname, DEBUG)

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
                "count_success": 0,
                "count_failures": 0,
                "time_profile": [],
            }
        )

        log.info("Update Job status to running.")
        write_job_file(current_path, job)

    # try:
    #     # log.debug("Job Inputs: %s" % json.dumps(job))

        # =========================== Parameters ===========================

        # ASTEROID_PATH: Diretório onde serão armazenados todos os arquivos referentes
        # aos Asteroids, dentro deste diretório serão criados diretorios para cada
        # Asteroid contendo seus arquivos de inputs e outputs.
        # Atenção: Precisar permitir uma quantidade grande de acessos de leitura e escrita simultaneas.
        ASTEROID_PATH = config["DEFAULT"].get("AsteroidPath")
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

        # TODO: Utilizar os parametros de BSP_PLanetary e LEAP Second do job.json.
        # BSP_PLANETARY = job["bsp_planetary"]["absolute_path"]
        # log.info("BSP_PLANETARY: [%s]" % BSP_PLANETARY)

        # LEAP_SECOND = job["leap_seconds"]["absolute_path"]
        # log.info("LEAP_SECOND: [%s]" % LEAP_SECOND)

        # CONDOR_JOB_TIME_LIMIT: Tempo Limite de execução de um job no HTCondor em Minutos.
        # Todo Job submetido e que estiver com JobStatus = 2 ou seja Running
        # será verificado o tempo de execução contando a partir do JobStartDate caso o tempo de execução seja maior
        # que o determinado nesta variavel ele será marcado para remoção.
        # default = 20
        CONDOR_JOB_TIME_LIMIT = int(config["DEFAULT"].getint("CondorJobTimeLimit", 20))
        log.info("Condor Job time Limit: [%s]" % CONDOR_JOB_TIME_LIMIT)

        # Remove resultados e inputs de execuções anteriores
        FORCE_REFRESH_INPUTS = bool(job.get("force_refresh_inputs", False))
        log.info("Force Refresh Inputs: [%s]" % FORCE_REFRESH_INPUTS)

        # Determina a validade dos arquivos de inputs.
        inputs_days_to_expire = int(job.get("inputs_days_to_expire", 5))
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

        #asteroids = asteroids[0:20]

        step_t1 = datetime.now(tz=timezone.utc)
        step_tdelta = step_t1 - step_t0

        job.update({"count_asteroids": len(asteroids)})

        log.info("Asteroids Count: %s" % job["count_asteroids"])

        log.info(
            "Retriving Asteroids Finished in %s"
            % humanize.naturaldelta(step_tdelta, minimum_unit="microseconds")
        )

        # Update Job File
        write_job_file(current_path, job)

        if job["count_asteroids"] == 0:
            raise ("No asteroid satisfying the criteria %s and %s. There is nothing to run." % (job["filter_type"], job["filter_value"]))

        # Lista de Jobs do Condor.
        htc_jobs = list()

        current_idx = 1

        for asteroid in asteroids:
            # print(asteroid)

            log.info(
                "---------------< Running: %s / %s >---------------"
                % (current_idx, len(asteroids))
            )
            log.info("Asteroid: [%s]" % asteroid["name"])

            a = Asteroid(
                id=asteroid["id"],
                name=asteroid["name"],
                number=asteroid["number"],
                base_dynclass=asteroid["base_dynclass"],
                dynclass=asteroid["dynclass"],
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
                    path=a.get_path(),
                )

                if htc_submited["success"]:
                    htc_job = htc_submited["jobs"][0]
                    htc_jobs.append(htc_job)

                    a.set_condor_job(
                        procid=htc_job["ProcId"], clusterid=htc_job["ClusterId"]
                    )

                    log.info("HTCondor Job submitted. %s" % str(htc_job))
                else:
                    log.error("Job submission failed on HTCondor. %s" % htc_submited)
                    # TODO: Marcar que deu erro no asteroid na submissão
                    current_idx += 1
                    continue

            except Exception as e:
                log.error(e)
                log.error("Job submission failed on HTCondor. %s" % htc_submited)
                # Se der erro na submissão do job continua para o proximo asteroid
                # TODO: Marcar que deu erro no asteroid na submissão
                current_idx += 1
                continue

            current_idx += 1

            del a
            # TODO: guardar em um dataset informação do asteroid e associado ao job. e se o asteroid foi executado ou ignorado.

        log.info("------------------------------------------------------------")
        log.info("All jobs have been submitted.")

        # ========================= Verifica todos os Jobs no HTCondor ============================
        condor_m = Condor()

        total_jobs = len(htc_jobs)
        finished_jobs = list()
        finished_ids = list()
        removed_jobs = list()
        count_idle = 0
        count_running = 0

        while len(finished_jobs) < total_jobs:

            # Sleep Necessário
            time.sleep(180)

            for htc_job in htc_jobs:

                if htc_job["ClusterId"] in finished_ids:
                    # Ignora as proximas etapas para este job.
                    # Se ele já estiver completo
                    continue

                try:
                    status = condor_m.get_job(
                        clusterId=htc_job["ClusterId"], procId=htc_job["ProcId"]
                    )

                    if "JobStatus" in status:

                        if status["JobStatus"] == "1":
                            # Job Idle
                            count_idle += 1

                        elif status["JobStatus"] == "2":
                            # Job Running
                            try:
                                # Verifica se o tempo de execução é maior que limite
                                htc_job_start = datetime.fromtimestamp(
                                    int(status["JobStartDate"])
                                )
                                now = datetime.now()
                                running_time = now - htc_job_start
                                # Converte o delta time para minutos
                                running_minutes = running_time.seconds % 3600 / 60.0

                                if running_minutes > int(CONDOR_JOB_TIME_LIMIT):
                                    # Job excedeu o tempo limite será marcado para remoção.
                                    condor_m.remove_job(
                                        clusterId=htc_job["ClusterId"],
                                        procId=htc_job["ProcId"],
                                    )
                                    log.warning(
                                        "Job timed out and will be removed. [%s]"
                                        % htc_job["ClusterId"]
                                    )

                                count_running += 1
                            except Exception as e:
                                log.error("Não retornou JobStartDate")
                                log.error(status)
                                log.error(e)

                                # TODO: Este remove é temporario só para identificar o problema
                                condor_m.remove_job(
                                    clusterId=htc_job["ClusterId"], procId=htc_job["ProcId"]
                                )

                        elif status["JobStatus"] == "3":
                            # Job Foi removido
                            finished_jobs.append(status)
                            finished_ids.append(htc_job["ClusterId"])
                            removed_jobs.append(htc_job)

                            log.debug("job has been removed. [%s]" % htc_job["ClusterId"])

                        elif status["JobStatus"] == "4":
                            finished_jobs.append(status)
                            finished_ids.append(htc_job["ClusterId"])
                            log.debug("Job Completed: [%s]" % status["ClusterId"])

                        elif status["JobStatus"] == "5":
                            # Job Held/Hold
                            # Marcar o Job para Remoção
                            condor_m.remove_job(
                                clusterId=htc_job["ClusterId"], procId=htc_job["ProcId"]
                            )
                            log.warning(
                                "Job in Hold has been marked for removal. [%s]"
                                % htc_job["ClusterId"]
                            )
                            # Não adicionar o job ao terminados só quando ele for removido.

                        else:
                            # Job Status não esperado marcar para remover
                            # Marcar o Job para Remoção
                            condor_m.remove_job(
                                clusterId=htc_job["ClusterId"], procId=htc_job["ProcId"]
                            )
                            log.warning(
                                "Job [%s] with Unexpected Status. has been marked for removal. [%s]"
                                % (htc_job["ClusterId"], status)
                            )
                            # Não adicionar o job ao terminados só quando ele for removido.

                except Exception as e:
                    log.error("Falhou ao consultar o condor")
                    log.error(e)

            log.info(
                "Total: Jobs [%s] Completed [%s] Running [%s] Idle [%s]."
                % (total_jobs, len(finished_jobs), count_running, count_idle)
            )
            count_idle = 0
            count_running = 0

        # Total de Jobs completos e removidos
        condor_job_completed = len(finished_jobs)
        condor_job_removed = len(removed_jobs)

        # Todos os jobs acabaram gerar um csv com os dados dos jobs retornados pelo HTCondor
        # Apenas para debug caso necessário.
        log.info("All Condor Jobs are done.")
        log.info("Jobs Removed : [%s]" % len(removed_jobs))
        df = pd.DataFrame(finished_jobs)
        htc_jobs_filepath = pathlib.Path(current_path, "htc_jobs.csv")
        df.to_csv(htc_jobs_filepath, encoding="utf-8", sep=";", index=False)
        del finished_jobs

        # ========================= Importacao dos resultados ============================
        log.info("------------------------------------------------------------")
        log.info("Start Importing Results.")

        l_consolidated = list()

        # Total de predições que forma ingeridas na tabela
        total_occultations = 0
        # Total de asteroids com algum evendo de ocultação no periodo.
        total_ast_occ = 0

        current_idx = 1
        for asteroid in asteroids:

            a = Asteroid(
                id=asteroid["id"], name=asteroid["name"], number=asteroid["number"],
            )
            a.set_log(logname)

            # Registrar as predições no banco de dados
            # Se o Asteroid tiver pelo menos um evento de occultação
            if a.predict_occultation is not None and a.predict_occultation["count"] > 0:

                # TODO: Coletar o tempo da execução da ingestão
                rowcount = a.register_occultations(PREDICT_START.date(), PREDICT_END.date())

                total_occultations += rowcount
                total_ast_occ += 1

                log.info("Asteroid: [%s] Occultations: [%s]" % (asteroid["name"], rowcount))

            # Aproveita o Loop em todos os asteroids para gerar um resumo consolidado de todos os asteroids envolvidos no Job
            l_consolidated.append(a.consiladate())

            #  Remove todos os arquivos gerados durante o processo, deixa apenas os inputs
            if not DEBUG:
                a.remove_outputs()

                del a

            current_idx += 1

        log.info(
            "Asteroids with Occultations: [%s] Occultations: [%s]"
            % (total_ast_occ, total_occultations)
        )

        # ========================= Consolidando resultados ============================
        log.info("Consolidating Job Results.")

        df_result = pd.DataFrame(
            l_consolidated,
            columns=[
                "ast_id",
                "name",
                "base_dynclass",
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
            ],
        )

        result_filepath = pathlib.Path(current_path, "job_consolidated.csv")
        df_result.to_csv(result_filepath, encoding="utf-8", sep=";", index=False)
        del df_result

        log.info("File with the consolidated Job data. [%s]" % result_filepath)

        # Status 3 = Completed
        job.update(
            {
                "status": "Completed",
                "ast_with_occ": total_ast_occ,
                "occultations": total_occultations,
                "condor_job_submited": total_jobs,
                "condor_job_completed": condor_job_completed,
                "condor_job_removed": condor_job_removed,
            }
        )

    except Exception as e:
        trace = traceback.format_exc()
        log.error(trace)
        log.error(e)

        # Status 4 = Failed
        job.update(
            {"status": "Failed", "error": str(e), "traceback": str(trace),}
        )

    finally:
        t1 = datetime.now(tz=timezone.utc)
        tdelta = t1 - t0

        # Calc average time by asteroid
        avg_exec_time_asteroid = 0
        if (job.get('count_asteroids') > 0):
            avg_exec_time_asteroid = int(tdelta.total_seconds() / job.get('count_asteroids'))

        job.update({
            "end": t1.isoformat(), 
            "exec_time": tdelta.total_seconds(),
            "h_exec_time": humanize.naturaldelta(tdelta),
            "avg_exec_time_asteroid": avg_exec_time_asteroid,
        })

        log.info("Update Job status.")
        write_job_file(current_path, job)

        # Altera o path de execução para o path original
        os.chdir(original_path)

        log.info("Execution Time: %s" % tdelta)
        log.info("Predict Occultation is done!.")

# Exemplo de execução do script
# python predict_occultation.py /lustre/t1/tmp/tno/predict_occultation/<job_folder>

# Exemplo de job.json
#{
#	"id": 6,
#	"status": "Submited",
#	"submit_time": "2023-03-24T10:20:00",
#	"path": "/lustre/t1/tmp/tno/predict_occultation/6_Eris",
#	"filter_type": "name",
#	"filter_value": "Eris",
#	"predict_start_date": "2023-01-01",
#	"predict_end_date": "2023-12-31",
#	"predict_step": 600,
#	"force_refresh_inputs": true,
#	"inputs_days_to_expire": 0,
#	"debug": true,
#	"bsp_planetary": {
#		"name": "de440",
#		"filename": "de440.bsp",
#		"absolute_path": "/lustre/t1/tmp/tno/bsp_planetary/de440.bsp"
#	},
#	"leap_seconds": {
#		"name": "naif0012",
#		"filename": "naif0012.tls",
#		"absolute_path": "/lustre/t1/tmp/tno/leap_seconds/naif0012.tls"
#	}
#}

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("path", help="Job Path")
    args = parser.parse_args()

    sys.exit(main(args.path))
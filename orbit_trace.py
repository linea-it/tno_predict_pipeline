# -*- coding: utf-8 -*-


import argparse
import configparser
import json
import os
import pathlib
import time
import traceback
from datetime import datetime, timezone

import humanize
import parsl

from asteroid import Asteroid
from library import (
    get_logger,
    read_inputs,
    retrieve_asteroids,
    write_job_file,
    ingest_observations,
    write_json,
)
from orbit_trace_apps import observed_positions, theoretical_positions
from parsl_config import htex_config

# Carrega as variaveis de configuração do arquivo config.ini
config = configparser.ConfigParser()
config.read("config.ini")


parser = argparse.ArgumentParser()
parser.add_argument("jobid", help="Job ID")
parser.add_argument("path", help="Job Path")
args = parser.parse_args()

# Job ID
jobid = int(args.jobid)

# Paths de execução
original_path = os.getcwd()
os.environ["EXECUTION_PATH"] = original_path

current_path = args.path

# Start Running Time
t0 = datetime.now(tz=timezone.utc)

# Create a Log file
log = get_logger(current_path, "orbit_trace.log")
log.info("--------------< DES Object Identifier >--------------")
log.info("Job ID: [%s]" % jobid)
log.info("Current Path: [%s]" % current_path)

# Altera o path de execução
# A raiz agora é o path passado como parametro.
os.chdir(current_path)

# Read Inputs from job.json
job = read_inputs(current_path, "job.json")

# TODO: Ler uma variavel de DEBUG a partir do job.json
# Essa variavel deve controlar o nivel de log e permitir que quando o debug esteja ativado os arquivos intermediarios de log não sejam apagados.

job.update(
    {
        "status": "Running",
        "start": t0.isoformat(),
        "end": None,
        "exec_time": 0,
        "count_success": 0,
        "count_failures": 0,
    }
)

log.info("Update Job status to running.")
write_job_file(current_path, job)

try:
    # log.debug('Job Inputs: %s' % json.dumps(job))

    # Setting Inputs
    DES_CATALOGS_BASEPATH = config["DEFAULT"].get("DesCatalogPath")
    log.info("DES_CATALOGS_BASEPATH: [%s]" % DES_CATALOGS_BASEPATH)

    BSP_PLANETARY = job["bsp_planetary"]["absolute_path"]
    log.info("BSP_PLANETARY: [%s]" % BSP_PLANETARY)

    LEAP_SECOND = job["leap_seconds"]["absolute_path"]
    log.info("LEAP_SECOND: [%s]" % LEAP_SECOND)

    DES_START_PERIOD = job["period"][0]
    log.info("DES_START_PERIOD: [%s]" % DES_START_PERIOD)

    DES_FINISH_PERIOD = job["period"][1]
    log.info("DES_FINISH_PERIOD: [%s]" % DES_FINISH_PERIOD)

    # Location of observatory: [longitude, latitude, elevation]
    OBSERVATORY_LOCATION = job["observatory_location"]
    log.info("OBSERVATORY_LOCATION: %s" % OBSERVATORY_LOCATION)

    MATCH_RADIUS = job["match_radius"]
    log.info("MATCH_RADIUS: [%s]" % MATCH_RADIUS)

    BSP_DAYS_TO_EXPIRE = job.get("bsp_days_to_expire", 60)

    # =========================== Asteroids ===========================

    # Retrieve Asteroids.
    log.info("Retriving Asteroids started")

    step_t0 = datetime.now(tz=timezone.utc)

    # Lista com os asteroids que falharem durante a execução.
    a_failed = list()

    asteroids = retrieve_asteroids(job["filter_type"], job["filter_value"])

    # asteroids = asteroids[0:5]

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
    write_job_file(current_path, job)

    # =========================== CCDs ===========================
    # Retrieve CCDs
    log.info("Retriving CCDs started")

    step_t0 = datetime.now(tz=timezone.utc)

    count_ccds = 0
    i = 0
    success_asteroids = list()
    ccd_ast_failed = 0
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
    write_job_file(current_path, job)

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
    write_job_file(current_path, job)

    # raise Exception("Parou aqui!")

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
    write_job_file(current_path, job)

    # =========================== Observed ===========================
    # Calculando as posições Observadas
    log.info("Calculating observed positions started")

    step_t0 = datetime.now(tz=timezone.utc)

    futures = list()
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

    # Agrupar os resultados.
    count_observations = 0
    obs_pos_ast_failed = 0
    for asteroid in asteroids:
        alias = asteroid_name.replace(" ", "_")
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

            # TODO: Adicionar uma variavel DEBUG, quando ela estiver True os logs não são apagados.
            # TODO: Remover os logs do diretório em caso de sucesso.
            # for p in pathlib.Path(asteroid["path"]).glob("des_obs*.log"):
            #     p.unlink()

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
    write_job_file(current_path, job)

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
        alias = asteroid_name.replace(" ", "_")

        # TODO: ERRO DO NOME PROVAVELMENTE ESTA AQUI ou ANTES DESSA ETAPA.
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
    write_job_file(current_path, job)

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

    job.update(
        {
            "end": t1.isoformat(),
            "exec_time": tdelta.total_seconds(),
            "h_exec_time": str(tdelta),
        }
    )

    log.info("Update Job status.")
    write_job_file(current_path, job)

    # Altera o path de execução para o path original
    os.chdir(original_path)

    log.info(
        "Asteroids Success: [%s] Failure: [%s]"
        % (job["count_success"], job["count_failures"])
    )

    log.info("Identification of DES object is done!.")
    log.info("Execution Time: %s" % tdelta)

# Como Utilízar:
# cd /archive/des/tno/dev/nima/pipeline
# source env.sh
# python orbit_trace.py 1 /archive/des/tno/dev/nima/pipeline/examples/orbit_trace_job


# 1999 LE31
# 2001 BL41
# 2001 XZ255


# 2022-01-26 17:26:07,654 [WARNING] Asteroid [2001 BL41] Failed on retrive asteroids BSP from JPL.
# ================================================================================

# Toolkit version: CSPICE66

# SPICE(SPKINSUFFDATA) --

# Insufficient ephemeris data has been loaded to compute the position of 2049036 relative to 0 (SOLAR SYSTEM BARYCENTER) at the ephemeris epoch 2013 FEB 11 09:06:01.735.

# spkpos_c --> SPKPOS --> SPKEZP --> SPKAPO --> SPKGPS

# ================================================================================


# [WARNING] Asteroid [1999 LE31] Failed on retrive asteroids BSP from JPL.
# ================================================================================

# Toolkit version: CSPICE66

# SPICE(EMPTYSTRING) --

# String "targ" has length zero.

# spkpos_c

# ================================================================================


# [WARNING] Asteroid [2001 XZ255] Failed on retrive asteroids BSP from JPL.
# ================================================================================

# Toolkit version: CSPICE66

# SPICE(EMPTYSTRING) --

# String "targ" has length zero.

# spkpos_c

# ================================================================================

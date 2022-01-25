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
from library import get_logger, read_inputs, retrieve_asteroids, write_job_file, ingest_observations
from orbit_trace_apps import observed_positions, theoretical_positions
from parsl_config import htex_config

# Carrega as variaveis de configuração do arquivo config.ini
config = configparser.ConfigParser()
config.read('config.ini')


parser = argparse.ArgumentParser()
parser.add_argument('jobid', help='Job ID')
parser.add_argument('path', help='Job Path')
args = parser.parse_args()

# Job ID
jobid = int(args.jobid)

# Paths de execução
original_path = os.getcwd()
os.environ['EXECUTION_PATH'] = original_path

current_path = args.path

# Start Running Time
t0 = datetime.now(tz=timezone.utc)

# Create a Log file
log = get_logger(current_path, 'orbit_trace.log')
log.info('--------------< DES Object Identifier >--------------')
log.info('Job ID: [%s]' % jobid)
log.info('Current Path: [%s]' % current_path)

# Altera o path de execução
# A raiz agora é o path passado como parametro.
os.chdir(current_path)

# Read Inputs from job.json
job = read_inputs(current_path, 'job.json')

job.update({
    'status': 'Running',
    'start': t0.isoformat()
})

log.info('Update Job status to running.')
write_job_file(current_path, job)

try:
    # log.debug('Job Inputs: %s' % json.dumps(job))

    # Setting Inputs
    DES_CATALOGS_BASEPATH = config['DEFAULT'].get('DesCatalogPath')
    log.info('DES_CATALOGS_BASEPATH: [%s]' % DES_CATALOGS_BASEPATH)

    BSP_PLANETARY = job['bsp_planetary']['absolute_path']
    log.info('BSP_PLANETARY: [%s]' % BSP_PLANETARY)

    LEAP_SECOND = job['leap_seconds']['absolute_path']
    log.info('LEAP_SECOND: [%s]' % LEAP_SECOND)

    DES_START_PERIOD = job['period'][0]
    log.info('DES_START_PERIOD: [%s]' % DES_START_PERIOD)

    DES_FINISH_PERIOD = job['period'][1]
    log.info('DES_FINISH_PERIOD: [%s]' % DES_FINISH_PERIOD)

    # Location of observatory: [longitude, latitude, elevation]
    OBSERVATORY_LOCATION = job['observatory_location']
    log.info('OBSERVATORY_LOCATION: %s' % OBSERVATORY_LOCATION)

    MATCH_RADIUS = job['match_radius']
    log.info('MATCH_RADIUS: [%s]' % MATCH_RADIUS)

    BSP_DAYS_TO_EXPIRE = job.get('bsp_days_to_expire', 60)

    # =========================== Asteroids ===========================

    # Retrieve Asteroids.
    log.info('Retriving Asteroids started')

    step_t0 = datetime.now(tz=timezone.utc)

    # Lista com os asteroids que falharem durante a execução.
    a_failed = list()

    asteroids = retrieve_asteroids(
        job['filter_type'],
        job['filter_value']
    )

    # asteroids = asteroids[0:120]

    job.update({'count_asteroids': len(asteroids)})

    step_t1 = datetime.now(tz=timezone.utc)
    step_tdelta = step_t1 - step_t0

    # Update Job time profile
    job['time_profile'].append(dict({
        'step': 'retrieve_asteroids',
        'start': step_t0.isoformat(),
        'end': step_t1.isoformat(),
        'exec_time': step_tdelta.total_seconds()
    }))

    job['processed_asteroids'] = len(asteroids)

    log.info('Asteroids Count: %s' % job['processed_asteroids'])

    log.info('Retriving Asteroids Finished in %s' %
             humanize.naturaldelta(step_tdelta, minimum_unit='microseconds'))

    # Update Job File
    write_job_file(current_path, job)

    # =========================== CCDs ===========================
    # Retrieve CCDs
    log.info('Retriving CCDs started')

    step_t0 = datetime.now(tz=timezone.utc)

    count_ccds = 0
    i = 0
    for asteroid in asteroids:

        if asteroid['status'] != 'failure':

            a = Asteroid(
                id=asteroid['id'],
                name=asteroid['name'],
                number=asteroid['number'],
                base_dynclass=asteroid['base_dynclass'],
                dynclass=asteroid['dynclass'],
            )

            a.set_log('orbit_trace')

            ccds = a.retrieve_ccds(LEAP_SECOND)
            count_ccds += len(ccds)

            if len(ccds) == 0:
                asteroid.update({'status': 'failure', 'ccds': []})
            else:
                asteroid.update({
                    'ccds': ccds,
                    'path': str(a.path)
                })

            log.debug('Asteroid: [%s] CCDs: [%s]' %
                      (asteroid['name'], len(asteroid['ccds'])))

            del a

        i += 1

        log.debug('Query CCDs: %s/%s' % (i, len(asteroids)))

    step_t1 = datetime.now(tz=timezone.utc)
    step_tdelta = step_t1 - step_t0

    # Update Job time profile
    job['time_profile'].append(dict({
        'step': 'retrieve_ccds',
        'start': step_t0.isoformat(),
        'end': step_t1.isoformat(),
        'exec_time': step_tdelta.total_seconds()
    }))

    job.update({'count_ccds': count_ccds})
    log.info('CCDs Count: %s' % count_ccds)

    log.info('Retriving CCDs Finished in %s' %
             humanize.naturaldelta(step_tdelta, minimum_unit='microseconds'))

    # Update Job File
    write_job_file(current_path, job)

    # =========================== BSP ===========================
    # Retrieve BSPs
    # Etapa sequencial
    log.info('Retriving BSP JPL started')

    step_t0 = datetime.now(tz=timezone.utc)

    i = 0
    for asteroid in asteroids:

        if asteroid['status'] != 'failure':

            a = Asteroid(
                id=asteroid['id'],
                name=asteroid['name'],
                number=asteroid['number'],
                base_dynclass=asteroid['base_dynclass'],
                dynclass=asteroid['dynclass'],
            )

            a.set_log('orbit_trace')

            have_bsp_jpl = a.check_bsp_jpl(
                start_period=DES_START_PERIOD,
                end_period=DES_FINISH_PERIOD,
                days_to_expire=BSP_DAYS_TO_EXPIRE
            )

            if have_bsp_jpl:
                # Recupera o SPKID que será usado na proxima etapa.
                spkid = a.get_spkid()
                # Recupera o Path para o BSP
                bsp_path = a.get_bsp_path()

                asteroid.update({
                    'spkid': spkid,
                    'bsp_path': str(bsp_path)
                })

            else:
                asteroid.update({'status': 'failure'})

        i += 1

        log.debug('BSPs JPL: %s/%s' % (i, len(asteroids)))

    step_t1 = datetime.now(tz=timezone.utc)
    step_tdelta = step_t1 - step_t0

    # Update Job time profile
    job['time_profile'].append(dict({
        'step': 'retrieve_bsp',
        'start': step_t0.isoformat(),
        'end': step_t1.isoformat(),
        'exec_time': step_tdelta.total_seconds()
    }))

    log.info('Retriving BSP JPL Finished %s' %
             humanize.naturaldelta(step_tdelta, minimum_unit='microseconds'))

    # Update Job File
    write_job_file(current_path, job)

    # =========================== Parsl ===========================
    log.info('Parsl Load started')
    # Load Parsl Configs
    parsl.clear()

    htex_config.run_dir = os.path.join(current_path, 'runinfo')

    # Verifica se a configuração tem a label htcondor
    try:
        # Htcondor config with full nodes
        htex_config.executors[0].provider.channel.script_dir = os.path.join(
            current_path, 'script_dir')

        # Adicionar o ID do processo ao arquivo de submissão do condor
        htex_config.executors[0].provider.scheduler_options += '+AppId = {}\n'.format(
            jobid)

        # Htcondor config with Limited nodes
        # htex_config.executors[1].provider.channel.script_dir = os.path.join(
        #     current_path, 'script_dir')

        # htex_config.executors[1].provider.scheduler_options += '+AppId = {}\n'.format(
        #     jobid)
    except:
        # Considera que é uma execução local
        pass

    parsl.load(htex_config)

    log.info('Parsl Load finished')

    # =========================== Theoretical ===========================
    # Calculando as posições teoricas
    log.info('Calculating theoretical positions started')

    step_t0 = datetime.now(tz=timezone.utc)

    futures = list()
    for asteroid in asteroids:
        if asteroid['status'] != 'failure':
            futures.append(theoretical_positions(
                asteroid, BSP_PLANETARY, LEAP_SECOND, OBSERVATORY_LOCATION))

    # Monitoramento parcial das tasks
    is_done = list()
    while is_done.count(True) != len(futures):
        is_done = list()
        for f in futures:
            is_done.append(f.done())
        log.debug('Theoretical Positions running: %s/%s' %
                  (is_done.count(True), len(futures)))
        time.sleep(60)

    # asteroids = [i.result() for i in futures]
    asteroids = list()
    theo_ast_failed = 0
    for task in futures:
        asteroid = task.result()
        if asteroid['status'] == 'failure':
            a_failed.append(asteroid)
            theo_ast_failed += 1
            log.warning('Asteroid [%s] %s' % (asteroid['error']))
        else:
            asteroids.append(asteroid)

    log.info('Calculating theoretical positions Finished %s. Asteroids Success [%s] Failed [%s]' % (
        humanize.naturaldelta(step_tdelta, minimum_unit='microseconds'),
        len(asteroids), theo_ast_failed))

    step_t1 = datetime.now(tz=timezone.utc)
    step_tdelta = step_t1 - step_t0

    # Update Job time profile
    job['time_profile'].append(dict({
        'step': 'theoretical_positions',
        'start': step_t0.isoformat(),
        'end': step_t1.isoformat(),
        'exec_time': step_tdelta.total_seconds()
    }))

    # Update Job File
    write_job_file(current_path, job)

    # =========================== Observed ===========================
    # Calculando as posições Observadas
    log.info('Calculating observed positions started')

    step_t0 = datetime.now(tz=timezone.utc)

    futures = list()
    for asteroid in asteroids:
        idx = 0
        for ccd in asteroid['ccds']:
            if ccd['theoretical_coordinates'] is not None:
                # Monta o path para os catalogos
                ccd['path'] = os.path.join(
                    DES_CATALOGS_BASEPATH, ccd['path'])

                futures.append(observed_positions(
                    idx=idx,
                    name=asteroid['name'],
                    asteroid_id=asteroid['id'],
                    ccd=ccd,
                    asteroid_path=asteroid['path'],
                    radius=MATCH_RADIUS,
                ))
                idx += 1

    # Monitoramento parcial das tasks
    is_done = list()
    while is_done.count(True) != len(futures):
        is_done = list()
        for f in futures:
            is_done.append(f.done())
        log.debug('Observed Positions running: %s/%s' %
                  (is_done.count(True), len(futures)))
        time.sleep(1)

    results = dict({})
    for task in futures:
        # TODO: Guardar o time profile, tempo gasto em cada observação.
        asteroid_name, ccd, obs_coordinates = task.result()

        alias = asteroid_name.replace(' ', '_')
        if alias not in results:
            results[alias] = dict({'ccds': list(), 'observations': list()})

        results[alias]['ccds'].append(ccd)
        if obs_coordinates is not None:
            results[alias]['observations'].append(obs_coordinates)

    # Agrupar os resultados.
    count_observations = 0
    obs_pos_ast_failed = 0
    for asteroid in asteroids:
        alias = asteroid_name.replace(' ', '_')
        asteroid.update({
            'ccds': results[alias]['ccds'],
            'observations': results[alias]['observations'],
            'observations_count': len(results[alias]['observations'])
        })
        count_observations += asteroid['observations_count']

        # Verificar se o asteroid teve algum erro
        # no Calculo das posições Observadas
        for ccd in asteroid['ccds']:
            if 'error' in ccd and ccd['error'] is not None:
                # Houve error em pelo menos 1 CCD do Asteroid.
                # Basta ter um erro nos ccds para considerar o Asteroid como falha.
                asteroid.update({
                    'status': 'failure',
                    'error': ccd['error']
                })

                log.warning('Asteroid [%s] %s' %
                            (asteroid['name'], ccd['error']))

                # Adiciona o Asteroid na lista de falhas
                a_failed.append(asteroid)

                obs_pos_ast_failed += 1

                # Interrompe o loop nos ccds.
                break

    # Remove do array de asteroid os que falharam.
    success_asteroids = list()
    for asteroid in asteroids:
        if asteroid['status'] != 'failure':
            success_asteroids.append(asteroid)

            # TODO: Remover os logs do diretório em caso de sucesso.
            for p in pathlib.Path(asteroid['path']).glob("des_obs*.log"):
                p.unlink()

    asteroids = success_asteroids

    job.update({'count_observations': count_observations})

    step_t1 = datetime.now(tz=timezone.utc)
    step_tdelta = step_t1 - step_t0

    # Update Job time profile
    job['time_profile'].append(dict({
        'step': 'observed_positions',
        'start': step_t0.isoformat(),
        'end': step_t1.isoformat(),
        'exec_time': step_tdelta.total_seconds()
    }))

    log.info('Observations Count: %s' % count_observations)
    log.info('Calculating observed positions Finished %s. Asteroids Success [%s] Failed [%s]' %
             (humanize.naturaldelta(step_tdelta, minimum_unit='microseconds'), len(asteroids), obs_pos_ast_failed))

    # Update Job File
    write_job_file(current_path, job)

    # =========================== Ingest Observations ===========================
    # Ingere as posições observadas no banco de dados
    # ETAPA SEQUENCIAL!
    log.info('Ingest the observations into the database started')

    step_t0 = datetime.now(tz=timezone.utc)

    ingested_obs = 0
    ingested_ast_failed = 0
    success_asteroids = list()
    for asteroid in asteroids:
        alias = asteroid_name.replace(' ', '_')
        observations = results[alias]['observations']

        if len(observations) > 0:
            result = ingest_observations(asteroid['path'], observations)
            asteroid.update({'ot_ing_obs': result})

            if 'error' in result:
                # Adiciona o Asteroid na lista de falhas
                a_failed.append(asteroid)
                ingested_ast_failed += 1
                log.warning('Asteroid [%s] %s' %
                            (asteroid['name'], result['error']))
                asteroid.update({
                    'status': 'failed',
                    'error': result['error']
                })
            else:
                success_asteroids.append(asteroid)
                ingested_obs += result['count']

    asteroids = success_asteroids

    step_t1 = datetime.now(tz=timezone.utc)
    step_tdelta = step_t1 - step_t0

    # Update Job time profile
    job['time_profile'].append(dict({
        'step': 'ingest_observations',
        'start': step_t0.isoformat(),
        'end': step_t1.isoformat(),
        'exec_time': step_tdelta.total_seconds()
    }))

    log.info('Observations Ingested: %s' % ingested_obs)
    log.info('Ingest the observations into the database Finished %s. Asteroids Success [%s] Failed [%s]' % (
             humanize.naturaldelta(step_tdelta, minimum_unit='microseconds'), len(asteroids), ingested_ast_failed))

    # Update Job File
    write_job_file(current_path, job)

    log.debug(asteroids)

    raise Exception('parou aqui')

    # TODO: Faltou adicionar os time profiles e resultados das etapas no Json do Asteroid.
    # TODO: Talvez guardar um arquivo com os ccds
    # TODO: Completar o Job

    # =========================== Asteroids Json ===========================
    log.info('Write Asteroid Data in json started')
    futures = list()
    for asteroid in asteroids:
        asteroid.update({'status': 'completed'})
        futures.append(write_asteroid_data(asteroid))

    asteroids = [i.result() for i in futures]

    log.info('Write Asteroid Data in json Finish %s' %
             humanize.naturaldelta(step_tdelta, minimum_unit='microseconds'))

    # Status 3 = Completed
    job.update({'status': 'Completed'})


except Exception as e:
    trace = traceback.format_exc()
    log.error(trace)
    log.error(e)

    # Status 4 = Failed
    job.update({
        'status': 'Failed',
        'error': str(e),
        'traceback': str(trace),
    })

finally:
    parsl.clear()

    t1 = datetime.now(tz=timezone.utc)
    tdelta = t1 - t0

    job.update({
        'end': t1.isoformat(),
        'exec_time': tdelta.total_seconds()
    })

    log.info('Update Job status.')
    write_job_file(current_path, job)

    # Altera o path de execução para o path original
    os.chdir(original_path)

    log.info('Execution Time: %s' % tdelta)
    log.info('Identification of DES object is done!.')


# Como Utilízar:
# cd /archive/des/tno/dev/nima/pipeline
# source env.sh
# python orbit_trace.py 1 /archive/des/tno/dev/nima/pipeline/examples/orbit_trace_job

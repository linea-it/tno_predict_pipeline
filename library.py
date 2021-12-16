#!/usr/bin/env python3
# -*- coding: utf-8 -*-

def get_logger(path, filename='refine.log'):
    import logging
    import os

    logfile = os.path.join(path, filename)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)
    log = logging.getLogger('refine')
    log.setLevel(logging.DEBUG)
    log.addHandler(file_handler)

    return log


def read_inputs(path, filename='job.json'):
    import os
    import json
    with open(os.path.join(path, filename)) as json_file:
        data = json.load(json_file)

    return data


def write_job_file(path, data):
    import json
    import os
    with open(os.path.join(path, 'job.json'), 'w') as json_file:
        json.dump(data, json_file)

def read_asteroid_json(path, asteroid_name):
    import pathlib
    import json

    alias = asteroid_name.replace(' ', '').replace('_', '')
    filename = "{}.json".format(alias)

    filepath = pathlib.Path(path, alias, filename)

    if filepath.exists():
        with open(filepath) as json_file:
            data = json.load(json_file)

            return data
    else:
        return None

def write_asteroid_json(path, asteroid_name, data):
    import pathlib
    import json

    alias = asteroid_name.replace(' ', '').replace('_', '')
    filename = "{}.json".format(alias)

    filepath = pathlib.Path(path, filename)

    with open(filepath, 'w') as json_file:
        json.dump(data, json_file)

def retrieve_asteroids(type, values):

    from dao import AsteroidDao

    asteroids = None
    if type == 'name':
        asteroids = AsteroidDao().get_asteroids_by_names(
            names=values.split(';')
        )
    elif type == 'dynclass':
        asteroids = AsteroidDao().get_asteroids_by_dynclass(
            dynclass=values
        )
    elif type == 'base_dynclass':
        asteroids = AsteroidDao().get_asteroids_by_base_dynclass(
            dynclass=values
        )


    for asteroid in asteroids:
        asteroid.update({
            'status': 'running',
        })

    return asteroids

def create_nima_input(name, number, period_start, period_end, path, template_path):

    import pathlib
    # Gerar o Input para Nima
    nima_input_file = pathlib.Path.joinpath(pathlib.Path(path), "nima_input.txt")

    with open(template_path) as file:
        data = file.read()

        # Substitui no template as tags {} pelo valor das variaveis.
        # Parametro Asteroid Name
        name = name.replace('_', '').replace(' ', '')
        data = data.replace('{name}', name.ljust(66))

        # Parametro Asteroid Number
        if number is None or number == '-':
            number = name
        data = data.replace('{number}', number.ljust(66))

        # Parametro Plot start e Plot end
        # data = data.replace('{plot_start_date}', period_start.ljust(66))
        year = int(period_end.split('-')[0]) - 1
        data = data.replace('{plot_end_year}', str(year))

        # Parametro BSP start e BSP end
        # data = data.replace('{bsp_start_date}', period_start.ljust(66))
        year = int(period_end.split('-')[0]) - 1
        data = data.replace('{bsp_end_year}', str(year))

        # Parametro Ephem start e Ephem end
        # data = data.replace('{ephem_start_date}', period_start.ljust(66))
        year = int(period_end.split('-')[0]) - 1
        data = data.replace('{ephem_end_year}', str(year))

        with open(nima_input_file, 'w') as new_file:
            new_file.write(data)

        return nima_input_file   

def ra2HMS(radeg, ndecimals=0):
    radeg = float(radeg)/15
    raH = int(radeg)
    raM = int((radeg - raH)*60)
    raS = 60*((radeg - raH)*60 - raM)
    style = '{:02d} {:02d} {:0' + str(ndecimals+3) + '.'+ str(ndecimals) + 'f}'
    RA = style.format(raH, raM, raS)
    return RA

def dec2DMS(decdeg, ndecimals=0):
    decdeg = float(decdeg)
    ds = '+'
    if decdeg < 0:
        ds, decdeg = '-', abs(decdeg)
    deg = int(decdeg)
    decM = abs(int((decdeg - deg)*60))
    decS = 60*(abs((decdeg - deg)*60) - decM)
    style = '{}{:02d} {:02d} {:0' + str(ndecimals+3) + '.'+ str(ndecimals) + 'f}'
    DEC = style.format(ds, deg, decM, decS)
    return DEC

def ra_hms_to_deg(ra):
    rs = 1

    H, M, S = [float(i) for i in ra.split()]
    if str(H)[0] == '-':
        rs, H = -1, abs(H)
    deg = (H*15) + (M/4) + (S/240)
    ra_deg = deg*rs

    return ra_deg

def dec_hms_to_deg(dec):
    ds = 1

    D, M, S = [float(i) for i in dec.split()]
    if str(D)[0] == '-':
        ds, D = -1, abs(D)
    deg = D + (M/60) + (S/3600)
    dec_deg = deg*ds

    return dec_deg



def submit_job(name, number, start, end, step, path):

    from condor import Condor
    import pathlib
    import configparser
    import os

    # Carrega as variaveis de configuração do arquivo config.ini
    config = configparser.ConfigParser()
    config.read(os.path.join(os.environ['EXECUTION_PATH'], 'config.ini'))

    # Catalog Datase URI
    # String de conexão com o banco de dados de catalogo onde devera ter uma tabela do 
    # Catalogo de estrelas GAIA DR2 que é utilizado pela imagem de predição.
    DB_URI = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
        config['CatalogDatabase'].get('DbUser'),
        config['CatalogDatabase'].get('DbPass'),
        config['CatalogDatabase'].get('DbHost'),
        config['CatalogDatabase'].get('DbPort'),
        config['CatalogDatabase'].get('DbName'),
    )

    path = path.rstrip("/")

    condor_m = Condor()

    # Se o asteroid tiver o atributo number usar um comando diferente para a execução.
    arguments = "/app/run.py {} {} {} --step {} --path {}".format(name, start, end, step, path)
    if number is not None:
        arguments = "/app/run.py {} {} {} --number {} --step {} --path {}".format(name, start, end, number, step, path)
   
    job = dict({
        "queues": 1,
        "submit_params": {
            "Universe": "docker",
            "Docker_image": "linea/praiaoccultation:v2.8.4",
            "executable": "/usr/local/bin/python",
            "arguments": arguments,
            "environment": "DB_URI={}".format(DB_URI),
            "docker_network_type": "host",
            "AppType": "TNO",
            "AppName": "Predict Occultation",
            "initialdir": pathlib.Path(path),
            "Log": pathlib.Path(path, "condor.log"),
            "Output": pathlib.Path(path, "condor.out"),
            "Error": pathlib.Path(path, "condor.err")
        }
    })

    result = condor_m.submit_job(job)

    return result

def count_lines(filepath):
    with open(filepath, 'r') as fp:
        num_lines = sum(1 for line in fp if line.rstrip())
        return num_lines

def ingest_occultations(asteroid_id, name, number, filepath, start_period, end_period):

    import pandas as pd
    from io import StringIO
    from dao import OccultationDao
    from library import ra_hms_to_deg, dec_hms_to_deg

    
    dao = OccultationDao()

    # Apaga as occultations já registradas para este asteroid antes de inserir.
    dao.delete_by_asteroid_id(asteroid_id, start_period, end_period)

    # Le o arquivo occultation table e cria um dataframe
    # occultation_date;ra_star_candidate;dec_star_candidate;ra_object;dec_object;ca;pa;vel;delta;g;j;h;k;long;loc_t;off_ra;off_de;pm;ct;f;e_ra;e_de;pmra;pmde
    df = pd.read_csv(
        filepath, 
        delimiter=";", 
        header=None, 
        skiprows=1,
        names=[
            "occultation_date", "ra_star_candidate", "dec_star_candidate", "ra_object", "dec_object", 
            "ca", "pa", "vel", "delta", "g", "j", "h", "k", "long", "loc_t", "off_ra", "off_de", "pm",
            "ct", "f", "e_ra", "e_de", "pmra", "pmde"
        ]
    )

    # Adiciona as colunas de coordenadas de target e star convertidas para degrees.
    df['ra_target_deg'] = df['ra_object'].apply(ra_hms_to_deg)
    df['dec_target_deg'] = df['dec_object'].apply(dec_hms_to_deg)
    df['ra_star_deg'] = df['ra_star_candidate'].apply(ra_hms_to_deg)
    df['dec_star_deg'] = df['dec_star_candidate'].apply(dec_hms_to_deg)

    # Adicionar colunas para asteroid id, name e number
    df['name'] = name
    df['number'] = number
    df['asteroid_id'] = asteroid_id

    # Remover valores como -- ou - 
    df['ct'] = df['ct'].str.replace('--', '')
    df['f'] = df['f'].str.replace('-', '')
  
    # Altera o nome das colunas
    df = df.rename(columns = {
        'occultation_date': 'date_time', 
        'ra_object': 'ra_target',
        'dec_object': 'dec_target',
        'ca': 'closest_approach',
        'pa': 'position_angle',
        'vel': 'velocity',
        'off_de': 'off_dec',
        'pm': 'proper_motion',
        'f': 'multiplicity_flag',
        'e_de': 'e_dec',
        'pmde': 'pmdec'
    })

    # Altera a ordem das colunas para coincidir com a da tabela
    df = df.reindex(columns=[
        "name", "number", "date_time", "ra_star_candidate", "dec_star_candidate", "ra_target", "dec_target", 
        "closest_approach", "position_angle", "velocity", "delta", "g", "j", "h", "k", "long", "loc_t", "off_ra", 
        "off_dec", "proper_motion", "ct", "multiplicity_flag", "e_ra", "e_dec", "pmra", "pmdec", "ra_star_deg", 
        "dec_star_deg", "ra_target_deg", "dec_target_deg", "asteroid_id"])

    data = StringIO()
    df.to_csv(
        data,
        sep="|",
        header=True,
        index=False,
    )
    data.seek(0)


    rowcount = dao.import_occultations(data)

    del df
    del data

    return rowcount

def has_expired(date, days=60):
    from datetime import datetime

    now = datetime.now()
    dif = now - date
    if dif.days < days:
        return False
    else:
        return True


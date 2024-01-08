# # -*- coding: utf-8 -*-
# from astropy.utils import iers
# from astropy.time import Time
# from astropy.utils.data import clear_download_cache
# clear_download_cache()
# finalFile = "https://maia.usno.navy.mil/ser7/finals2000A.all"
# iers.IERS.iers_table = iers.IERS_A.open(finalFile)


def get_logger(path, filename="refine.log", debug=False):
    import logging
    import os

    logfile = os.path.join(path, filename)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)
    log = logging.getLogger(filename.split(".log")[0])
    if debug:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    log.addHandler(file_handler)

    return log


def read_inputs(path, filename="job.json"):
    import os
    import json

    with open(os.path.join(path, filename)) as json_file:
        data = json.load(json_file)

    return data


def write_job_file(path, data):
    import json
    import os

    with open(os.path.join(path, "job.json"), "w") as json_file:
        json.dump(data, json_file)


def write_json(filepath, data):
    import json
    import os

    with open(os.path.join(filepath), "w") as json_file:
        json.dump(data, json_file)


# def read_asteroid_json(path, asteroid_name):
#     import pathlib
#     import json

#     alias = asteroid_name.replace(' ', '').replace('_', '')
#     filename = "{}.json".format(alias)

#     filepath = pathlib.Path(path, alias, filename)

#     if filepath.exists():
#         with open(filepath) as json_file:
#             data = json.load(json_file)

#             return data
#     else:
#         return None


# def write_asteroid_json(path, asteroid_name, data):
#     import pathlib
#     import json

#     alias = asteroid_name.replace(' ', '').replace('_', '')
#     filename = "{}.json".format(alias)

#     filepath = pathlib.Path(path, filename)

#     with open(filepath, 'w') as json_file:
#         json.dump(data, json_file)


def retrieve_asteroids(type, values):

    from dao import AsteroidDao

    dao = AsteroidDao()

    asteroids = list()

    if type == "name":
        asteroids = dao.get_asteroids_by_names(names=values.split(";"))
    elif type == "dynclass":
        asteroids = dao.get_asteroids_by_dynclass(dynclass=values)
    elif type == "base_dynclass":
        asteroids = dao.get_asteroids_by_base_dynclass(dynclass=values)

    for asteroid in asteroids:
        asteroid.update(
            {"status": "running",}
        )

    return asteroids


def ra2HMS(radeg, ndecimals=0):
    radeg = float(radeg) / 15
    raH = int(radeg)
    raM = int((radeg - raH) * 60)
    raS = 60 * ((radeg - raH) * 60 - raM)
    style = "{:02d} {:02d} {:0" + str(ndecimals + 3) + "." + str(ndecimals) + "f}"
    RA = style.format(raH, raM, raS)
    return RA


def dec2DMS(decdeg, ndecimals=0):
    decdeg = float(decdeg)
    ds = "+"
    if decdeg < 0:
        ds, decdeg = "-", abs(decdeg)
    deg = int(decdeg)
    decM = abs(int((decdeg - deg) * 60))
    decS = 60 * (abs((decdeg - deg) * 60) - decM)
    style = "{}{:02d} {:02d} {:0" + str(ndecimals + 3) + "." + str(ndecimals) + "f}"
    DEC = style.format(ds, deg, decM, decS)
    return DEC


def ra_hms_to_deg(ra):
    rs = 1

    H, M, S = [float(i) for i in ra.split()]
    if str(H)[0] == "-":
        rs, H = -1, abs(H)
    deg = (H * 15) + (M / 4) + (S / 240)
    ra_deg = deg * rs

    return ra_deg


def dec_hms_to_deg(dec):
    ds = 1

    D, M, S = [float(i) for i in dec.split()]
    if str(D)[0] == "-":
        ds, D = -1, abs(D)
    deg = D + (M / 60) + (S / 3600)
    dec_deg = deg * ds

    return dec_deg


def submit_job(name, number, start, end, step, path):

    from condor import Condor
    import pathlib
    import configparser
    import os

    # Carrega as variaveis de configuração do arquivo config.ini
    config = configparser.ConfigParser()
    config.read(os.path.join(os.environ["EXECUTION_PATH"], "config.ini"))

    # Catalog Datase URI
    # String de conexão com o banco de dados de catalogo onde devera ter uma tabela do
    # Catalogo de estrelas GAIA DR2 que é utilizado pela imagem de predição.
    DB_URI = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
        config["CatalogDatabase"].get("DbUser"),
        config["CatalogDatabase"].get("DbPass"),
        config["CatalogDatabase"].get("DbHost"),
        config["CatalogDatabase"].get("DbPort"),
        config["CatalogDatabase"].get("DbName"),
    )

    path = path.rstrip("/")

    condor_m = Condor()

    # Se o asteroid tiver o atributo number usar um comando diferente para a execução.
    arguments = "/app/run.py {} {} {} --step {} --path {}".format(
        name, start, end, step, path
    )
    if number is not None:
        arguments = "/app/run.py {} {} {} --number {} --step {} --path {}".format(
            name, start, end, number, step, path
        )

    job = dict(
        {
            "queues": 1,
            "submit_params": {
                "Universe": "docker",
                "Docker_image": "linea/praiaoccultation:v2.8.8",
                "executable": "/usr/local/bin/python",
                "arguments": arguments,
                "environment": "DB_URI={}".format(DB_URI),
                "docker_network_type": "host",
                "AppType": "TNO",
                "AppName": "Predict Occultation",
                "initialdir": pathlib.Path(path),
                "Log": pathlib.Path(path, "condor.log"),
                "Output": pathlib.Path(path, "condor.out"),
                "Error": pathlib.Path(path, "condor.err"),
            },
        }
    )

    result = condor_m.submit_job(job)

    return result


def count_lines(filepath):
    with open(filepath, "r") as fp:
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
            "occultation_date",
            "ra_star_candidate",
            "dec_star_candidate",
            "ra_object",
            "dec_object",
            "ca",
            "pa",
            "vel",
            "delta",
            "g",
            "j",
            "h",
            "k",
            "long",
            "loc_t",
            "off_ra",
            "off_de",
            "pm",
            "ct",
            "f",
            "e_ra",
            "e_de",
            "pmra",
            "pmde",
        ],
    )

    # Adiciona as colunas de coordenadas de target e star convertidas para degrees.
    df["ra_target_deg"] = df["ra_object"].apply(ra_hms_to_deg)
    df["dec_target_deg"] = df["dec_object"].apply(dec_hms_to_deg)
    df["ra_star_deg"] = df["ra_star_candidate"].apply(ra_hms_to_deg)
    df["dec_star_deg"] = df["dec_star_candidate"].apply(dec_hms_to_deg)

    # Adicionar colunas para asteroid id, name e number
    df["name"] = name
    df["number"] = number
    df["asteroid_id"] = asteroid_id

    # Remover valores como -- ou -
    df["ct"] = df["ct"].str.replace("--", "")
    df["f"] = df["f"].str.replace("-", "")

    # Altera o nome das colunas
    df = df.rename(
        columns={
            "occultation_date": "date_time",
            "ra_object": "ra_target",
            "dec_object": "dec_target",
            "ca": "closest_approach",
            "pa": "position_angle",
            "vel": "velocity",
            "off_de": "off_dec",
            "pm": "proper_motion",
            "f": "multiplicity_flag",
            "e_de": "e_dec",
            "pmde": "pmdec",
        }
    )

    # Altera a ordem das colunas para coincidir com a da tabela
    df = df.reindex(
        columns=[
            "name",
            "number",
            "date_time",
            "ra_star_candidate",
            "dec_star_candidate",
            "ra_target",
            "dec_target",
            "closest_approach",
            "position_angle",
            "velocity",
            "delta",
            "g",
            "j",
            "h",
            "k",
            "long",
            "loc_t",
            "off_ra",
            "off_dec",
            "proper_motion",
            "ct",
            "multiplicity_flag",
            "e_ra",
            "e_dec",
            "pmra",
            "pmdec",
            "ra_star_deg",
            "dec_star_deg",
            "ra_target_deg",
            "dec_target_deg",
            "asteroid_id",
        ]
    )

    data = StringIO()
    df.to_csv(
        data, sep="|", header=True, index=False,
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


def date_to_jd(date_obs, exptime, leap_second):
    """Aplica uma correção a data de observação e converte para data juliana

    Correção para os CCDs do DES:
        date = date_obs + 0.5 * (exptime + 1.05)

    Args:
        date_obs (datetime): Data de observação do CCD "date_obs"
        exptime (float): Tempo de exposição do CCD "exptime"
        lead_second (str): Path para o arquivo leap second a ser utilizado por exemplo: '/archive/lead_second/naif0012.tls'

    Returns:
        str: Data de observação corrigida e convertida para julian date.
    """
    import spiceypy as spice

    # Calcula a correção
    correction = (exptime + 1.05) / 2

    # Carrega o lead second na lib spicepy
    spice.furnsh(leap_second)

    # Converte a date time para JD
    date_et = spice.utc2et(str(date_obs).split("+")[0] + " UTC")
    date_jdutc = spice.et2utc(date_et, "J", 14)

    # Remove a string JD retornada pela lib
    midtime = date_jdutc.replace("JD ", "")

    # Soma a correção
    jd = float(midtime) + correction / 86400

    spice.kclear()

    return jd


def geo_topo_vector(longitude, latitude, elevation, jd):
    """
    Transformation from [longitude, latitude, elevation] to [x,y,z]
    """

    from astropy.coordinates import GCRS, EarthLocation
    from astropy.time import Time
    import numpy as np

    loc = EarthLocation(longitude, latitude, elevation)

    time = Time(jd, scale="utc", format="jd")
    itrs = loc.get_itrs(obstime=time)
    gcrs = itrs.transform_to(GCRS(obstime=time))

    r = gcrs.cartesian

    # convert from m to km
    x = r.x.value / 1000.0
    y = r.y.value / 1000.0
    z = r.z.value / 1000.0

    return np.array([x, y, z])


def compute_theoretical_positions(
    spkid, ccds, bsp_jpl, bsp_planetary, leap_second, location, log
):

    log.info("Compute Theoretical Positions")
    log.debug("SPKID: %s" % spkid)

    # try:
    #     log.info("Download finals2000A.all")
    #     from astropy.utils import iers
    #     from astropy.time import Time

    #     finalFile = "https://maia.usno.navy.mil/ser7/finals2000A.all"
    #     iers.IERS.iers_table = iers.IERS_A.open(finalFile)

    # except Exception as e:
    #     log.error("Failed to download finals2000A.all Error: %s" % e)

    import spiceypy as spice
    import numpy as np
    from library import geo_topo_vector

    # Limpar o Kernel antes por garantia.
    spice.kclear()
    # TODO: Provavelmente esta etapa é que causa a lentidão desta operação
    # Por que carrega o arquivo de ephemeris planetarias que é pesado
    # Load the asteroid and planetary ephemeris and the leap second (in order)
    log.info("Loading BSP Planetary: %s" % bsp_planetary)
    spice.furnsh(bsp_planetary)

    log.info("Loading Leap Second: %s" % leap_second)
    spice.furnsh(leap_second)

    log.info("Loading JPL BSP %s" % bsp_jpl)
    spice.furnsh(bsp_jpl)

    results = list()

    for ccd in ccds:

        date_jd = ccd["date_jd"]

        # Convert dates from JD to et format. "JD" is added due to spice requirement
        date_et = spice.utc2et(str(date_jd) + " JD UTC")
        # log.debug("date_et: %s" % date_et)

        # Compute geocentric positions (x,y,z) in km for each date with light time correction
        r_geo, lt_ast = spice.spkpos(spkid, date_et, "J2000", "LT", "399")
        # log.debug("r_geo: %s" % r_geo)

        lon, lat, ele = location
        l_ra, l_dec = [], []

        # Convert from geocentric to topocentric coordinates
        r_topo = r_geo - geo_topo_vector(lon, lat, ele, float(date_jd))
        # log.debug("r_topo: %s" % r_topo)

        # Convert rectangular coordinates (x,y,z) to range, right ascension, and declination.
        d, rarad, decrad = spice.recrad(r_topo)
        # log.debug("rarad: %s" % rarad)
        # log.debug("decrad: %s" % decrad)

        # Transform RA and Decl. from radians to degrees.
        ra = np.degrees(rarad)
        # log.debug("ra: %s" % ra)

        dec = np.degrees(decrad)
        # log.debug("dec: %s" % dec)

        ccd.update(
            {
                "date_et": date_et,
                "geocentric_positions": list(r_geo),
                "topocentric_positions": list(r_topo),
                "theoretical_coordinates": [ra, dec],
            }
        )

        log.info(
            "CCD ID: %s Theoretical Coord: %s"
            % (ccd["id"], ccd["theoretical_coordinates"])
        )

        results.append(ccd)

    spice.kclear()

    log.info("Theoretical Positions Completed")

    return results


def ingest_observations(path, observations):

    import pathlib
    import pandas as pd
    from io import StringIO
    from dao import ObservationDao
    from datetime import datetime, timezone

    result = dict()
    tp0 = datetime.now(tz=timezone.utc)

    try:
        dao = ObservationDao()

        # Apaga as observations já registradas para este asteroid antes de inserir.
        name = observations[0]["name"]
        dao.delete_by_asteroid_name(name)

        df_obs = pd.DataFrame(
            observations,
            columns=[
                "name",
                "date_obs",
                "date_jd",
                "ra",
                "dec",
                "offset_ra",
                "offset_dec",
                "mag_psf",
                "mag_psf_err",
                "asteroid_id",
                "ccd_id",
            ],
        )
        df_obs["created_at"] = datetime.now(tz=timezone.utc)
        # Guarda uma copia das observações no diretório do Asteroid.
        filepath = pathlib.Path(path, "des_obs.csv")
        df_obs.to_csv(filepath, sep=";", header=True, index=False)

        # TODO: Tratar os dados se necessário
        data = StringIO()
        df_obs.to_csv(
            data, sep="|", header=True, index=False,
        )
        data.seek(0)

        tablename = "des_observation"

        # Sql Copy com todas as colunas que vão ser importadas e o formato do csv.
        sql = (
            "COPY %s (name, date_obs, date_jd, ra, dec, offset_ra, offset_dec, mag_psf, mag_psf_err, asteroid_id, ccd_id, created_at) FROM STDIN with (FORMAT CSV, DELIMITER '|', HEADER);"
            % tablename
        )

        rowcount = dao.import_with_copy_expert(sql, data)

        del dao
        del df_obs
        del data

        result.update({"count": rowcount, "filename": filepath.name})

    except Exception as e:
        msg = "Failed on ingest des observation in database. Error: %s" % e
        result.update(
            {"error": msg,}
        )

    finally:
        tp1 = datetime.now(tz=timezone.utc)
        result.update({"tp_start": tp0.isoformat(), "tp_finish": tp1.isoformat()})

        return result


def get_configs():
    import configparser
    import os
    # Carrega as variaveis de configuração do arquivo config.ini
    execution_path = os.environ['EXECUTION_PATH']
    config_filepath = os.path.join(execution_path, 'config.ini')
    config = configparser.ConfigParser()
    config.read(config_filepath)

    return config
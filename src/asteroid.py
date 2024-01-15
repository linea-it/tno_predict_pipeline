import configparser
import os
import pathlib
import json
import datetime
from datetime import datetime as dt, timezone
from external_inputs.asteroid_external_inputs import AsteroidExternalInputs
from library import (
    has_expired,
    ra2HMS,
    dec2DMS,
    ra_hms_to_deg,
    dec_hms_to_deg,
    date_to_jd,
)
from dao import ObservationDao, OccultationDao, AsteroidDao
import csv
import pandas as pd
import numpy as np
from io import StringIO
import logging
from external_inputs.jpl import get_bsp_from_jpl, findSPKID
from occviz import occultation_path_coeff

def serialize(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, datetime.date):
        serial = obj.isoformat()
        return serial

    if isinstance(obj, datetime.time):
        serial = obj.isoformat()
        return serial

    if isinstance(obj, pathlib.PosixPath):
        serial = str(obj)
        return serial

    if isinstance(obj, logging.Logger):
        return None

    return obj.__dict__


class Asteroid:
    __log = None
    __BSP_START_PERIOD = "2012-01-01"
    __BSP_YEARS_AHEAD = 1
    __BSP_YEARS_BEHIND = 1
    __BSP_DAYS_TO_EXPIRE = 60
    __BASE_PATH = None

    # Status
    # 0 = undefined
    # 1 = Success
    # 2 = Failure
    # status = 0

    id = None
    name = None
    number = None
    spkid = None
    alias = None
    dynclass = None
    base_dynclass = None
    path = None

    ot_query_ccds = dict()
    ot_theo_pos = dict()
    ot_ing_obs = dict()

    condor_job = None
    #des_observations = {}
    #bsp_jpl = {}
    #observations = {}
    #orbital_elements = {}
    #refine_orbit = {}
    #predict_occultation = {}
    #ingest_occultations = {}

    # messages = list()
    exec_time = None

    def __init__(
        self,
        name,
        id=None,
        number=None,
        base_dynclass=None,
        dynclass=None,
        base_path=None,
    ):
        self.name = name
        self.set_alias(name)

        # Status
        # 0 = undefined
        # 1 = Success
        # 2 = Failure
        self.status = 0

        self.des_observations = dict()
        self.bsp_jpl = dict()
        self.observations = dict()
        self.orbital_elements = dict()
        self.refine_orbit = dict()
        self.predict_occultation = dict()
        self.ingest_occultations = dict()

        self.id = id
        if number is not None and number != "-" and number != "":
            self.number = str(number)
        self.base_dynclass = base_dynclass
        self.dynclass = dynclass

        # Cria ou recupera o path do asteroid
        self.__BASE_PATH = base_path
        self.path = self.get_or_create_dir()

        # Verifica se existe arquivo json para o objeto se existir carrega o conteudo na classe
        json_data = self.read_asteroid_json()

        # print('JSON_DATA: ', json_data)

        self.messages = list()
        self.__dict__.update(json_data)

        # TODO: O Correto é que a leitura do json seja feita por ultimo.
        # Mas no momento existe jsons com valores antigos e invalidos
        # para corrigir isto vou sobrescrever alguns campos apos a leitura do json.
        # Os campos que vem por parametro vem do banco de dados que é sempre atualizado.

        # if number is not None and number != "-" and number != "":
        #     self.number = str(number)

        # if base_dynclass is not None:
        #     self.base_dynclass = base_dynclass

        # if dynclass is not None:
        #     self.dynclass = dynclass

    def __getitem__(self, item):
        return self.__dict__[item]

    def set_alias(self, name):
        self.alias = name.replace(" ", "").replace("_", "").replace("/", "")

    def to_dict(self):
        dcopy = self.__dict__.copy()
        return dict(
            (key, value)
            for (key, value) in dcopy.items()
            if key.startswith("_") is False
        )

    def set_log(self, logname):
        if isinstance(logname, str):
            self.__logname = logname
            self.__log = logging.getLogger(logname)
        else:
            self.__log = logname

    def get_log(self):
        if self.__log is None:
            self.__log = logging.getLogger(self.__logname)

        return self.__log

    def get_base_path(self):
        base_path = self.__BASE_PATH
        if not base_path:
            # Carrega as variaveis de configuração do arquivo config.ini
            config = configparser.ConfigParser()
            config.read(os.path.join(os.environ["EXECUTION_PATH"], "config.ini"))
            base_path = config["DEFAULT"].get("AsteroidPath")
            self.__BASE_PATH = base_path

        return base_path

    def get_jpl_email(self):
        # Carrega as variaveis de configuração do arquivo config.ini
        config = configparser.ConfigParser()
        config.read(os.path.join(os.environ["EXECUTION_PATH"], "config.ini"))
        JPL_EMAIL = config["DEFAULT"].get("JplEmail", "sso-portal@linea.gov.br")
        return JPL_EMAIL

    def get_or_create_dir(self):
        """Criar o diretório para o asteroid.

        Args:
            name ([type]): [description]
        """
        asteroid_path = pathlib.Path.joinpath(
            pathlib.Path(self.get_base_path()), self.alias
        )

        pathlib.Path(asteroid_path).mkdir(parents=True, exist_ok=True)

        return asteroid_path

    def get_path(self):
        return self.path

    def set_success(self):
        self.status = 1

    def set_failure(self):
        self.status = 2

    def read_asteroid_json(self):
        filename = "{}.json".format(self.alias)

        filepath = pathlib.Path(self.path, filename)

        if not filepath.exists():
            # Se não existir um json para este asteroid cria um.
            self.write_asteroid_json()

        with open(filepath) as json_file:
            return json.load(json_file)

    def write_asteroid_json(self):
        filename = "{}.json".format(self.alias)
        filepath = pathlib.Path(self.path, filename)

        d = self.to_dict()

        with open(filepath, "w") as json_file:
            json.dump(d, json_file, default=serialize)

    def set_condor_job(self, procid, clusterid):
        self.condor_job = dict({"proc_id": procid, "cluster_id": clusterid})

        self.write_asteroid_json()

    def get_bsp_path(self):
        filename = self.get_bsp_filename()
        filepath = pathlib.Path.joinpath(pathlib.Path(self.path), filename)

        return filepath

    def calculate_bsp_start_period(self, start_period):
        years_behind = int(self.__BSP_YEARS_BEHIND)
        start_period = dt.strptime(str(start_period), "%Y-%m-%d")
        start = dt(year=start_period.year - years_behind, month=1, day=1)

        return start.strftime("%Y-%m-%d")

    def calculate_bsp_end_period(self, end_period):
        years_ahead = int(self.__BSP_YEARS_AHEAD)
        end_period = dt.strptime(str(end_period), "%Y-%m-%d")
        end = dt(year=end_period.year + years_ahead, month=12, day=31)

        return end.strftime("%Y-%m-%d")

    def get_bsp_filename(self):
        return f"{self.alias}.bsp"

    def download_jpl_bsp(self, end_period, force=False, start_period=None):
        """
        Exemplo do retorno:
            {
                'source': 'JPL',
                'filename': '2010BJ35.bsp',
                'size': 225280,
                'start_period': '2012-01-01',
                'end_period': '2024-12-31',
                'dw_start': '2021-11-23T20:27:21.014818+00:00',
                'dw_finish': '2021-11-23T20:27:23.887789+00:00',
                'dw_time': 2.872971
            }
        """
        log = self.get_log()
        log.debug("Downloading BSP JPL started")
        log.debug(self.get_bsp_path())

        bsp_path = self.get_bsp_path()
        bsp_filename = self.get_bsp_filename()

        if force is True and bsp_path.exists():
            # Remove o arquivo se já existir e force=True
            # Um novo download será realizado.
            bsp_path.unlink()
            log.debug("Removed old bsp: [%s]" % (not bsp_path.exists()))

        if start_period is None:
            start_period = self.__BSP_START_PERIOD
        else:
            start_period = self.calculate_bsp_start_period(start_period)

        t0 = dt.now(tz=timezone.utc)
        end_period = self.calculate_bsp_end_period(end_period)

        try:
            bsp_path = get_bsp_from_jpl(
                self.name, start_period, end_period, self.path, bsp_filename
            )
            t1 = dt.now(tz=timezone.utc)
            tdelta = t1 - t0

            data = dict(
                {
                    "source": "JPL",
                    "filename": bsp_path.name,
                    "size": bsp_path.stat().st_size,
                    "start_period": start_period,
                    "end_period": end_period,
                    "dw_start": t0.isoformat(),
                    "dw_finish": t1.isoformat(),
                    "dw_time": tdelta.total_seconds(),
                    "downloaded_in_this_run": True,
                }
            )

            log.info("Asteroid [%s] BSP Downloaded in %s" % (self.name, tdelta))

            return data
        except Exception as e:
            log.warning("Failed to Download BSP. Error: [%s]" % e)
            return None

    def check_bsp_jpl(self, end_period, days_to_expire=None, start_period=None):
        log = self.get_log()

        tp0 = dt.now(tz=timezone.utc)

        try:
            log.debug("Asteroid [%s] Checking BSP JPL" % self.name)

            if days_to_expire is None:
                days_to_expire = self.__BSP_DAYS_TO_EXPIRE

            if days_to_expire == 0:
                # Força o download de um novo BSP
                self.bsp_jpl = dict()
                log.debug("Force Download days to expire = 0")

            bsp_jpl = dict()

            # Verificar insformações sobre o BSP no Json
            if self.bsp_jpl and "filename" in self.bsp_jpl:
                # Já existe Informações de BSP baixado

                # Path para o arquivo BSP
                bsp_path = self.get_bsp_path()

                # Verificar se o arquivo BSP existe
                if bsp_path.exists():
                    # Arquivo Existe Verificar se está na validade usando da data de criação do arquivo
                    dt_creation = dt.fromtimestamp(bsp_path.stat().st_mtime)

                    if not has_expired(dt_creation, days_to_expire):
                        # BSP Está na validade
                        # Verificar se o periodo do bsp atende ao periodo da execução.
                        bsp_start_period = dt.strptime(
                            self.bsp_jpl["start_period"], "%Y-%m-%d"
                        ).date()
                        exec_start_period = dt.strptime(
                            str(start_period), "%Y-%m-%d"
                        ).date()

                        bsp_end_period = dt.strptime(
                            self.bsp_jpl["end_period"], "%Y-%m-%d"
                        ).date()
                        exec_end_period = dt.strptime(
                            str(end_period), "%Y-%m-%d"
                        ).date()

                        if (
                            bsp_start_period < exec_start_period
                            and bsp_end_period > exec_end_period
                        ):
                            # O BSP contem dados para um periodo maior que o necessário para execução
                            # BSP que já existe atente todos os critérios não será necessário um novo Download.
                            bsp_jpl = self.bsp_jpl
                            bsp_jpl["downloaded_in_this_run"] = False
                            log.info(
                                "Asteroid [%s] Pre-existing BSP is still valid and will be reused."
                                % self.name
                            )

            if not bsp_jpl:
                # Fazer um novo Download do BSP
                bsp_jpl = self.download_jpl_bsp(
                    start_period=start_period, end_period=end_period, force=True
                )

                # Toda vez que baixar um novo BSP recalcular o SPKID
                self.spkid = None
                self.get_spkid()

            if bsp_jpl:
                # Atualiza os dados do bsp
                self.bsp_jpl = bsp_jpl

                return True
            else:
                msg = "BSP JPL file was not created."
                self.bsp_jpl = dict({"message": msg})

                log.warning("Asteroid [%s] %s" % (self.name, msg))
                return False

        except Exception as e:
            msg = "Failed in the BSP JPL stage. Error: %s" % e

            self.bsp_jpl = dict({"message": msg})
            log.error("Asteroid [%s] %s" % (self.name, msg))

            return False

        finally:
            # Atualiza o Json do Asteroid
            tp1 = dt.now(tz=timezone.utc)

            self.bsp_jpl.update(
                {"tp_start": tp0.isoformat(), "tp_finish": tp1.isoformat()}
            )

            self.write_asteroid_json()

    def check_orbital_elements(self, days_to_expire=None):
        log = self.get_log()

        tp0 = dt.now(tz=timezone.utc)

        try:
            log.info("Checking Orbital Elements")

            aei = AsteroidExternalInputs(
                name=self.name,
                number=self.number,
                asteroid_path=self.path,
                logname=self.__logname,
            )

            if days_to_expire is None:
                days_to_expire = aei.MPC_DAYS_TO_EXPIRE

            orb_ele = dict()

            # Verificar insformações sobre Orbital Elements no Json
            if (
                self.orbital_elements
                and "filename" in self.orbital_elements
            ):
                # Já existe Informações de Orbital Elements

                # Path para o arquivo
                orb_path = pathlib.Path.joinpath(
                    pathlib.Path(self.path), self.orbital_elements["filename"]
                )

                # Verificar se o arquivo Orbital Elements existe
                if orb_path.exists():
                    # Arquivo Existe Verificar se está na validade usando da data de criação do arquivo
                    dt_creation = dt.fromtimestamp(orb_path.stat().st_mtime)

                    if not has_expired(dt_creation, days_to_expire):
                        # O Arquivo existe e esta na validade não será necessário um novo Download.
                        orb_ele = self.orbital_elements
                        orb_ele["downloaded_in_this_run"] = False
                        log.info(
                            "Pre-existing Orbital Elements is still valid and will be reused."
                        )

            if not orb_ele:
                # Fazer um novo Download
                # Tenta primeiro vindo do AstDys
                orb_ele = aei.download_astdys_orbital_elements(force=True)

                if not orb_ele:
                    # Tenta no MPC
                    orb_ele = aei.download_mpc_orbital_elements(force=True)

            if orb_ele:
                # Atualiza os dados
                self.orbital_elements = orb_ele

                return True
            else:
                msg = "Orbital Elements file was not created."
                self.orbital_elements = dict({"message": msg})

                log.warning("Asteroid [%s] %s" % (self.name, msg))

                return False

        except Exception as e:
            msg = "Failed in the Orbital Elements stage. Error: %s" % e

            self.orbital_elements = dict({"message": msg})
            log.error("Asteroid [%s] %s" % (self.name, msg))

            return False

        finally:
            # Atualiza o Json do Asteroid

            tp1 = dt.now(tz=timezone.utc)

            self.orbital_elements.update(
                {"tp_start": tp0.isoformat(), "tp_finish": tp1.isoformat()}
            )

            self.write_asteroid_json()

    def check_observations(self, days_to_expire=None):
        log = self.get_log()

        tp0 = dt.now(tz=timezone.utc)

        try:
            log.info("Checking Observations")

            aei = AsteroidExternalInputs(
                name=self.name,
                number=self.number,
                asteroid_path=self.path,
                logname=self.__logname,
            )

            if days_to_expire is None:
                days_to_expire = aei.MPC_DAYS_TO_EXPIRE

            observations = dict()

            # Verificar insformações sobre Observations no Json
            if self.observations and "filename" in self.observations:
                # Já existe Informações

                # Path para o arquivo
                obs_path = pathlib.Path.joinpath(
                    pathlib.Path(self.path), self.observations["filename"]
                )

                # Verificar se o arquivo Observations existe
                if obs_path.exists():
                    # Arquivo Existe Verificar se está na validade usando da data de criação do arquivo
                    dt_creation = dt.fromtimestamp(obs_path.stat().st_mtime)

                    if not has_expired(dt_creation, days_to_expire):
                        # O Arquivo existe e esta na validade não será necessário um novo Download.
                        observations = self.observations
                        observations["downloaded_in_this_run"] = False
                        log.info(
                            "Pre-existing Observations is still valid and will be reused."
                        )

            if not observations:
                # Fazer um novo Download
                # Tenta primeiro vindo do AstDys
                observations = aei.download_astdys_observations(force=True)

                if not observations:
                    # Tenta no MPC
                    observations = aei.download_mpc_orbital_elements(force=True)

            if observations:
                # Atualiza os dados
                self.observations = observations

                return True
            else:
                msg = "Observations file was not created."
                self.observations = dict({"message": msg})

                log.warning("Asteroid [%s] %s" % (self.name, msg))

                return False

        except Exception as e:
            msg = "Failed in the Observations stage. Error: %s" % e

            self.observations = dict({"message": msg})
            log.error("Asteroid [%s] %s" % (self.name, msg))

            return False

        finally:
            # Atualiza o Json do Asteroid

            tp1 = dt.now(tz=timezone.utc)

            self.observations.update(
                {"tp_start": tp0.isoformat(), "tp_finish": tp1.isoformat()}
            )

            self.write_asteroid_json()

    def get_des_observations_path(self):
        filename = "{}.txt".format(self.alias)

        return pathlib.Path.joinpath(pathlib.Path(self.path), filename)

    def retrieve_des_observations(self, force=False):
        log = self.get_log()
        log.info("Retriving DES Observations started")

        fpath = self.get_des_observations_path()
        if fpath.exists() and force is True:
            fpath.unlink()

        t0 = dt.now(tz=timezone.utc)

        # TODO: Verificar primeiro se existe o arquivo de observações criado
        # Pela etapa Orbit Trace. ai evita a query no banco.

        # Se for a primeira vez ou o arquivo tiver expirado
        # Executa a query na tabela de observações.
        dao = ObservationDao()
        observations = dao.get_observations_by_name(self.name)
        del dao

        rows = ""
        rows_count = len(observations)
        for obs in observations:
            row = dict(
                {
                    "ra": ra2HMS(obs["ra"], 4).ljust(15),
                    "dec": dec2DMS(obs["dec"], 3).ljust(16),
                    "mag": "{:.3f}".format(obs["mag_psf"]).ljust(8),
                    "mjd": "{:.8f}".format(float(obs["date_jd"])).ljust(18),
                    "obs": "W84".ljust(5),
                    "cat": "V",
                }
            )

            rows += "{ra}{dec}{mag}{mjd}{obs}{cat}\n".format(**row)

        log.info("Creating DES observations file.")
        with open(fpath, "w") as f:
            f.write(rows)

        t1 = dt.now(tz=timezone.utc)
        tdelta = t1 - t0

        if fpath.exists():
            log.info(
                "DES observations Count [%s] File. [%s]" % (rows_count, str(fpath))
            )

            return dict(
                {
                    "filename": fpath.name,
                    "size": fpath.stat().st_size,
                    "count": int(rows_count),
                    "start": t0.isoformat(),
                    "finish": t1.isoformat(),
                    "exec_time": tdelta.total_seconds(),
                    "generated_in_this_run": True,
                }
            )

        else:
            return None

    def check_des_observations(self, days_to_expire=90):
        log = self.get_log()

        tp0 = dt.now(tz=timezone.utc)

        try:
            log.info("Checking DES Observations")

            observations = dict()
            # Verificar insformações sobre DES Observations no Json
            if (
                self.des_observations 
                and "filename" in self.des_observations
            ):
                # Já existe Informações

                # Path para o arquivo
                obs_path = pathlib.Path.joinpath(
                    pathlib.Path(self.path), self.des_observations["filename"]
                )

                # Verificar se o arquivo DES Observations existe
                if obs_path.exists():
                    # Arquivo Existe Verificar se está na validade usando da data de criação do arquivo
                    dt_creation = dt.fromtimestamp(obs_path.stat().st_mtime)

                    if not has_expired(dt_creation, days_to_expire):
                        # O Arquivo existe e esta na validade não será necessário uma novo consulta.
                        observations = self.des_observations
                        observations["generated_in_this_run"] = False
                        log.info(
                            "Pre-existing DES Observations is still valid and will be reused."
                        )

            if not observations:
                # Fazer uma nova Consulta
                observations = self.retrieve_des_observations(force=True)

            if not observations:
                # Atualiza os dados
                self.des_observations = observations

                if self.des_observations["count"] > 0:
                    return True
                else:
                    return False

            else:
                msg = "DES Observations file was not created."
                self.des_observations = dict({"message": msg})
                return False

                log.warning("Asteroid [%s] %s" % (self.name, msg))

        except Exception as e:
            msg = "Failed in the DES Observations stage. Error: %s" % e

            self.des_observations = dict({"message": msg})
            log.error("Asteroid [%s] %s" % (self.name, msg))

            return False

        finally:
            tp1 = dt.now(tz=timezone.utc)

            self.des_observations.update(
                {"tp_start": tp0.isoformat(), "tp_finish": tp1.isoformat()}
            )

            # Atualiza o Json do Asteroid
            self.write_asteroid_json()

    def remove_previus_results(self, remove_inputs=False):
        log = self.get_log()
        log.debug("Removing previous results.")

        t0 = dt.now(tz=timezone.utc)

        removed_files = list()

        ignore_files = [
            "{}.json".format(self.alias),
            "{}.bsp".format(self.alias),
            "{}.eq0".format(self.alias),
            "{}.eqm".format(self.alias),
            "{}.rwo".format(self.alias),
            "{}.rwm".format(self.alias),
            "{}.txt".format(self.alias),
        ]

        # Se a opção remove_inputs for verdadeira
        # Todos os arquivos do diretório serão removidos a flag ignore_files será ignorada
        if remove_inputs is True:
            ignore_files = []

            # Ao remover os arquivos de input limpa tb os metadados sobre os arquivos
            self.condor_job = None
            self.des_observations = {}
            self.bsp_jpl = {}
            self.observations = {}
            self.orbital_elements = {}

        path = pathlib.Path(self.path)
        for f in path.iterdir():
            if f.name not in ignore_files:
                removed_files.append(f.name)
                f.unlink()

        # Limpa os metadados das etapas de resultado
        self.refine_orbit = {}
        self.predict_occultation = {}
        self.ingest_occultations = {}

        # Atualiza o Json do Asteroid
        self.write_asteroid_json()

        t1 = dt.now(tz=timezone.utc)
        tdelta = t1 - t0

        # log.debug("Removed Files: [%s]" % ", ".join(removed_files))
        log.info("Removed [%s] files in %s" % (len(removed_files), tdelta))

    def register_occultations(self, start_period: str, end_period: str, jobid: int):
        log = self.get_log()

        if "filename" not in self.predict_occultation:
            # Nao executou a etapa de predicao.
            return 0

        try:
            t0 = dt.now(tz=timezone.utc)

            predict_table_path = pathlib.Path(
                self.path, self.predict_occultation["filename"]
            )

            dao = OccultationDao()

            # Apaga as occultations já registradas para este asteroid antes de inserir.
            dao.delete_by_asteroid_id(self.id, start_period, end_period)

            # Le o arquivo occultation table e cria um dataframe
            # occultation_date;ra_star_candidate;dec_star_candidate;ra_object;dec_object;ca;pa;vel;delta;g;j;h;k;long;loc_t;off_ra;off_de;pm;ct;f;e_ra;e_de;pmra;pmde
            df = pd.read_csv(
                predict_table_path,
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
            df["name"] = self.name
            df["number"] = self.number
            df["asteroid_id"] = self.id

            # Remover valores como -- ou -
            df["ct"] = df["ct"].str.replace("--", "")
            df["f"] = df["f"].str.replace("-", "")

            df["created_at"] = dt.now(tz=timezone.utc)

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

            coeff_paths = []

            # Para cada Ocultacao e necessario calcular o occultation path. 
            for row in df.to_dict(orient="records"):

                new_row = {
                    "have_path_coeff": False,
                    "occ_path_min_longitude": None,
                    "occ_path_max_longitude":  None,
                    "occ_path_min_latitude":  None,
                    "occ_path_max_latitude":  None,   
                    "occ_path_is_nightside":  None,
                    "occ_path_coeff": {}
                }

                occ_coeff = occultation_path_coeff(
                    date_time=dt.strptime(row['date_time'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc).isoformat(), 
                    ra_star_candidate=row['ra_star_candidate'],
                    dec_star_candidate=row['dec_star_candidate'],
                    closest_approach=row['closest_approach'], 
                    position_angle=row['position_angle'], 
                    velocity=row['velocity'], 
                    delta_distance=row['delta'], 
                    offset_ra=row['off_ra'], 
                    offset_dec=row['off_dec'], 
                    object_diameter=row.get('diameter', None), 
                    ring_radius=None
                )


                if len(occ_coeff['coeff_latitude']) > 0  and len(occ_coeff['coeff_longitude']) > 0:
                    new_row.update({
                        "have_path_coeff": True,
                        "occ_path_min_longitude": float(occ_coeff['min_longitude']) if occ_coeff['min_longitude'] != None else None,
                        "occ_path_max_longitude": float(occ_coeff['max_longitude']) if occ_coeff['max_longitude'] != None else None,
                        "occ_path_min_latitude":  float(occ_coeff['min_latitude']) if occ_coeff['min_latitude'] != None else None,
                        "occ_path_max_latitude":  float(occ_coeff['max_latitude']) if occ_coeff['max_latitude'] != None else None,   
                        "occ_path_is_nightside":  bool(occ_coeff['nightside']),
                        "occ_path_coeff": json.dumps(occ_coeff)
                    })
              
                coeff_paths.append(new_row)        


            df_coeff = pd.DataFrame.from_dict(coeff_paths)

            df["have_path_coeff"] = df_coeff["have_path_coeff"]
            df["occ_path_max_longitude"] = df_coeff["occ_path_max_longitude"]
            df["occ_path_min_longitude"] = df_coeff["occ_path_min_longitude"]
            df["occ_path_coeff"] = df_coeff["occ_path_coeff"]
            df["occ_path_is_nightside"] = df_coeff["occ_path_is_nightside"]
            df["occ_path_max_latitude"] = df_coeff["occ_path_max_latitude"]
            df["occ_path_min_latitude"] = df_coeff["occ_path_min_latitude"]
           
            # log.debug(df.columns.values.tolist())

            # Adiciona algumas informacoes de Proveniencia a cada evento de predicao
            # base_dynclass, dynclass, catalog, predict_step, bsp_source, obs_source, orb_ele_source, bsp_planetary, leap_seconds, nima , job_id  

            df["base_dynclass"] = self.base_dynclass
            df["dynclass"] = self.dynclass
            
            df["catalog"] = self.predict_occultation['catalog']
            df["predict_step"] = self.predict_occultation['predict_step']
            df["bsp_source"] = self.bsp_jpl['source']
            df["obs_source"] = self.observations['source']
            df["orb_ele_source"] = self.orbital_elements['source']
            df["bsp_planetary"] = self.predict_occultation['bsp_planetary']
            df["leap_seconds"] = self.predict_occultation['leap_seconds']
            df["nima"] = self.predict_occultation['nima']
            df["job_id"] = jobid
            
            # Correcao de valores nao validos
            # Fix https://github.com/linea-it/tno_pipelines/issues/10.
            df.loc[df['j'] == 50, 'j'] = None
            df.loc[df['h'] == 50, 'h'] = None
            df.loc[df['k'] == 50, 'k'] = None

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
                    "created_at",
                    "have_path_coeff", 
                    "occ_path_max_longitude",
                    "occ_path_min_longitude",
                    "occ_path_coeff",
                    "occ_path_is_nightside",
                    "occ_path_max_latitude",
                    "occ_path_min_latitude",
                    "base_dynclass", 
                    "dynclass", 
                    "catalog", 
                    "predict_step", 
                    "bsp_source", 
                    "obs_source",
                    "orb_ele_source", 
                    "bsp_planetary", 
                    "leap_seconds", 
                    "nima", 
                    "job_id",  
                ]
            )

            # ATENCAO! Sobrescreve o arquivo occultation_table.csv
            df.to_csv(predict_table_path, index=False, sep=";")

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
            del df_coeff
            del data
            del dao

            self.ingest_occultations.update({"count": rowcount})

            return rowcount
        except Exception as e:
            msg = "Failed in Ingest Occultations stage. Error: %s" % e

            self.ingest_occultations.update({"message": msg})
            log.error("Asteroid [%s] %s" % (self.name, msg))
            self.set_failure()
            return 0

        finally:
            t1 = dt.now(tz=timezone.utc)
            tdelta = t1 - t0

            self.ingest_occultations.update(
                {
                    "start": t0.isoformat(),
                    "finish": t1.isoformat(),
                    "exec_time": tdelta.total_seconds(),
                }
            )
            
            # Atualiza o Json do Asteroid
            self.write_asteroid_json()

    def consiladate(self):
        log = self.get_log()

        a = dict(
            {
                "ast_id": self.id,
                "name": self.name,
                "number": self.number,
                "base_dynclass": self.base_dynclass,
                "dynclass": self.dynclass,
            }
        )

        try:
            exec_time = 0

            if self.des_observations:
                if "message" in self.des_observations:
                    # self.messages.append(self.des_observations["message"])
                    # self.set_failure()
                    pass
                else:
                    a.update(
                        {
                            "des_obs": self.des_observations["count"],
                            "des_obs_start": self.des_observations["start"],
                            "des_obs_finish": self.des_observations["finish"],
                            "des_obs_exec_time": self.des_observations["exec_time"],
                            "des_obs_gen_run": self.des_observations.get(
                                "generated_in_this_run", False
                            ),
                            "des_obs_tp_start": self.des_observations["tp_start"],
                            "des_obs_tp_finish": self.des_observations["tp_finish"],
                        }
                    )

                    exec_time += float(self.des_observations["exec_time"])

            if self.bsp_jpl:
                if "message" in self.bsp_jpl:
                    self.messages.append(self.bsp_jpl["message"])
                    self.set_failure()
                else:
                    a.update(
                        {
                            "bsp_jpl_start": self.bsp_jpl["dw_start"],
                            "bsp_jpl_finish": self.bsp_jpl["dw_finish"],
                            "bsp_jpl_dw_time": self.bsp_jpl["dw_time"],
                            "bsp_jpl_dw_run": self.bsp_jpl.get(
                                "downloaded_in_this_run", False
                            ),
                            "bsp_jpl_tp_start": self.bsp_jpl["tp_start"],
                            "bsp_jpl_tp_finish": self.bsp_jpl["tp_finish"],
                        }
                    )

                    exec_time += float(self.bsp_jpl["dw_time"])

            if self.observations:
                if "message" in self.observations:
                    self.messages.append(self.observations["message"])
                    self.set_failure()
                else:
                    a.update(
                        {
                            "obs_source": self.observations["source"],
                            "obs_start": self.observations["dw_start"],
                            "obs_finish": self.observations["dw_finish"],
                            "obs_dw_time": self.observations["dw_time"],
                            "obs_dw_run": self.observations.get(
                                "downloaded_in_this_run", False
                            ),
                            "obs_tp_start": self.observations["tp_start"],
                            "obs_tp_finish": self.observations["tp_finish"],
                        }
                    )

                    exec_time += float(self.observations["dw_time"])

            if self.orbital_elements:
                if "message" in self.orbital_elements:
                    self.messages.append(self.orbital_elements["message"])
                    self.set_failure()
                else:
                    a.update(
                        {
                            "orb_ele_source": self.orbital_elements["source"],
                            "orb_ele_start": self.orbital_elements["dw_start"],
                            "orb_ele_finish": self.orbital_elements["dw_finish"],
                            "orb_ele_dw_time": self.orbital_elements["dw_time"],
                            "orb_ele_dw_run": self.orbital_elements.get(
                                "downloaded_in_this_run", False
                            ),
                            "orb_ele_tp_start": self.orbital_elements["tp_start"],
                            "orb_ele_tp_finish": self.orbital_elements["tp_finish"],
                        }
                    )

                    exec_time += float(self.orbital_elements["dw_time"])

            if self.refine_orbit:
                if "message" in self.refine_orbit:
                    self.messages.append(self.refine_orbit["message"])
                else:
                    a.update(
                        {
                            "ref_orb_start": self.refine_orbit["start"],
                            "ref_orb_finish": self.refine_orbit["finish"],
                            "ref_orb_exec_time": self.refine_orbit["exec_time"],
                        }
                    )

                    exec_time += float(self.refine_orbit["exec_time"])

            if self.predict_occultation:
                if "message" in self.predict_occultation:
                    self.messages.append(self.predict_occultation["message"])
                    self.set_failure()
                else:
                    a.update(
                        {
                            "pre_occ_count": int(self.predict_occultation["count"]),
                            "pre_occ_start": self.predict_occultation["start"],
                            "pre_occ_finish": self.predict_occultation["finish"],
                            "pre_occ_exec_time": self.predict_occultation["exec_time"],
                        }
                    )
                    exec_time += float(self.predict_occultation["exec_time"])

            # Ingest Occultations
            if "message" in self.ingest_occultations:
                self.messages.append(self.ingest_occultations["message"])
                self.set_failure()

            a.update(
                {
                    "ing_occ_count": int(self.ingest_occultations.get("count", 0)),
                    "ing_occ_start": self.ingest_occultations.get("start", None),
                    "ing_occ_finish": self.ingest_occultations.get("finish", None),
                    "ing_occ_exec_time": self.ingest_occultations.get("exec_time", 0),
                }
            )

            exec_time += float(self.ingest_occultations.get("exec_time", 0))

            # Tempo total de execução do Asteroid
            a["exec_time"] = exec_time
            self.exec_time = exec_time

        except Exception as e:
            self.set_failure()

            msg = "Failed to consolidate asteroid results. Error: %s" % e
            log.error("Asteroid [%s] %s" % (self.name, msg))
            self.messages.append(msg)

        finally:
            # Junta todas as mensagens Warnings e Errors em uma string separada por ;
            self.messages = list(dict.fromkeys(self.messages))
            a["messages"] = "; ".join(self.messages)

            if self.status == 2:
                a["status"] = 2
            else:
                # Se nao foi marcado com erro ate aqui entao 
                # Pode ser considerado Sucesso!
                self.set_success()
                a["status"] = 1

            # Atualiza o Json do Asteroid
            self.write_asteroid_json()
            return a

    def remove_outputs(self):
        log = self.get_log()
        # log.debug("Removing Outputs.")

        t0 = dt.now(tz=timezone.utc)

        removed_files = list()

        ignore_files = [
            "{}.json".format(self.alias),
            "{}.bsp".format(self.alias),
            "{}.eq0".format(self.alias),
            "{}.eqm".format(self.alias),
            "{}.rwo".format(self.alias),
            "{}.rwm".format(self.alias),
            "{}.txt".format(self.alias),
            "diff_bsp-ni.png",
            "diff_nima_jpl_Dec.png",
            "diff_nima_jpl_RA.png",
            "omc_sep_recent.png",
            "omc_sep_all.png",
            "occultation_table.csv",
        ]

        path = pathlib.Path(self.path)
        for f in path.iterdir():
            if f.name not in ignore_files:
                removed_files.append(f.name)
                f.unlink()

        t1 = dt.now(tz=timezone.utc)
        tdelta = t1 - t0

        # log.debug("Removed Files: [%s]" % ", ".join(removed_files))
        log.info("Removed [%s] files in %s" % (len(removed_files), tdelta))

    def retrieve_ccds(self, leap_second):
        log = self.get_log()

        # Limpa o cache de resultados anteriores, esta etapa
        # Para esta etapa sempre será executada uma query nova.
        self.ot_query_ccds = dict()

        tp0 = dt.now(tz=timezone.utc)

        try:
            # log.info("Retriving CCDs")

            dao = AsteroidDao()
            ccds = dao.ccds_by_asteroid(self.name)

            for ccd in ccds:
                # Correção no path dos ccds, para ficar igual ao ambiente do linea
                path = ccd["path"].replace("OPS/", "")
                path = path.replace("/red/immask", "/cat")
                filename = ccd["filename"].replace("immasked.fits", "red-fullcat.fits")
                # print(str(ccd["date_obs"].astimezone(timezone.utc)))
                ccd.update(
                    {
                        "date_obs": str(ccd["date_obs"].astimezone(timezone.utc)),
                        "date_jd": date_to_jd(
                            str(ccd["date_obs"].astimezone(timezone.utc)).strip(),
                            ccd["exptime"],
                            leap_second,
                        ),
                        "path": path,
                        "filename": filename,
                    }
                )

            self.ot_query_ccds.update({"count": len(ccds)})

            return ccds

        except Exception as e:
            msg = "Failed in the Retriving CCDs stage. Error: %s" % e

            self.ot_query_ccds = dict({"message": msg})
            log.error("Asteroid [%s] %s" % (self.name, msg))

        finally:
            # Atualiza o Json do Asteroid

            tp1 = dt.now(tz=timezone.utc)

            self.ot_query_ccds.update(
                {"tp_start": tp0.isoformat(), "tp_finish": tp1.isoformat()}
            )

            self.write_asteroid_json()

    def get_spkid(self):
        log = self.get_log()

        if self.spkid is None or self.spkid == "":
            bsp_path = self.get_bsp_path()

            if self.bsp_jpl and bsp_path.exists():
                log.debug("Search the SPKID from bsp file.")

                try:
                    spkid = findSPKID(str(bsp_path))

                    if spkid is None or spkid == "":
                        self.spkid = None
                        log.warning(
                            "Asteroid [%s] Could not identify the SPKID." % self.name
                        )
                    else:
                        self.spkid = spkid
                        log.debug("Asteroid [%s] SPKID [%s]." % (self.name, self.spkid))

                except Exception as e:
                    self.spkid = None
                    log.warning("Asteroid [%s] %s." % (self.name, e))

                self.write_asteroid_json()

        return self.spkid

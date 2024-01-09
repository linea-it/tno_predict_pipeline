from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.sql import select, delete, update, insert, and_
import collections
import configparser
import os
from sqlalchemy.pool import NullPool
import warnings
from sqlalchemy import exc as sa_exc
from datetime import datetime, timezone, timedelta


class MissingDBURIException(Exception):
    pass


class Dao():

    con = None

    def get_db_uri(self):

        # DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME
        # db_uri = "postgresql+psycopg2://%s:%s@%s:%s/%s" % (
        #     "postgres", "postgres", "172.18.0.2", "5432", "tno_v2")

        # DB_URI=postgresql+psycopg2://USER:PASS@HOST:PORT/DB_NAME
        try:
            db_uri = os.environ['DB_URI_ADMIN']
            return db_uri
        except:
            raise MissingDBURIException(
                "Required environment variable with URI to access the database."
                "example DB_URI_ADMIN=postgresql+psycopg2://USER:PASS@HOST:PORT/DB_NAME")

    def get_db_engine(self):
        # Carrega as variaveis de configuração do arquivo config.ini
        config = configparser.ConfigParser()
        config.read(os.path.join(os.environ['EXECUTION_PATH'], 'config.ini'))

        engine = create_engine(
            self.get_db_uri(),
            poolclass=NullPool
        )

        return engine

    def get_con(self):
        if self.con is None:
            engine = self.get_db_engine()
            self.con = engine.connect()

        return self.con

    def get_table(self, tablename, schema=None):

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=sa_exc.SAWarning)

            engine = self.get_db_engine()
            tbl = Table(
                tablename, MetaData(engine), autoload=True, schema=schema)
            return tbl

    def fetch_all_dict(self, stm):

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=sa_exc.SAWarning)

            engine = self.get_db_engine()
            with engine.connect() as con:

                queryset = con.execute(stm)

                rows = list()
                for row in queryset:
                    d = dict(collections.OrderedDict(row))
                    rows.append(d)

                return rows

    def fetch_one_dict(self, stm):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=sa_exc.SAWarning)

            engine = self.get_db_engine()
            with engine.connect() as con:

                queryset = con.execute(stm).fetchone()

                if queryset is not None:
                    d = dict(collections.OrderedDict(queryset))
                    return d
                else:
                    return None

    def get_job_by_id(self, id):

        tbl = self.get_table(tablename='des_astrometryjob')
        stm = select(tbl.c).where(and_(tbl.c.id == int(id)))

        return self.fetch_one_dict(stm)

    def import_with_copy_expert(self, sql, data):
        """
            This method is recommended for importing large volumes of data. using the postgresql COPY method.

            The method is useful to handle all the parameters that PostgreSQL makes available 
            in COPY statement: https://www.postgresql.org/docs/current/sql-copy.html

            it is necessary that the from clause is reading from STDIN.

            example: 
            sql = COPY <table> (<columns) FROM STDIN with (FORMAT CSV, DELIMITER '|', HEADER);

            Parameters:
                sql (str): The sql statement should be in the form COPY table '.
                data (file-like ): a file-like object to read or write
            Returns:
                rowcount (int):  the number of rows that the last execute*() produced (for DQL statements like SELECT) or affected (for DML statements like UPDATE or INSERT)

        References: 
            https://www.psycopg.org/docs/cursor.html#cursor.copy_from
            https://stackoverflow.com/questions/30050097/copy-data-from-csv-to-postgresql-using-python
            https://stackoverflow.com/questions/13125236/sqlalchemy-psycopg2-and-postgresql-copy
        """

        # with warnings.catch_warnings():
        #     warnings.simplefilter("ignore", category=sa_exc.SAWarning)

        #     engine = self.get_db_engine()
        #     with engine.raw_connection() as connection:
        #         try:
        #             cursor = connection.cursor()
        #             cursor.copy_expert(sql, data)
        #             connection.commit()

        #             cursor.close()
        #             return cursor.rowcount
        #         except Exception as e:
        #             connection.rollback()
        #             raise (e)
        #         finally:
        #             connection.close()

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=sa_exc.SAWarning)

            connection = self.get_db_engine().raw_connection()
            try:
                cursor = connection.cursor()
                cursor.copy_expert(sql, data)
                connection.commit()

                cursor.close()
                return cursor.rowcount
            except Exception as e:
                connection.rollback()
                raise (e)
            finally:
                connection.close()


class AsteroidDao(Dao):
    def __init__(self):
        super(AsteroidDao, self).__init__()

        self.tbl = self.get_table('tno_asteroid')

    def get_asteroids_by_names(self, names):

        stm = select(self.tbl.c).where(and_(self.tbl.c.name.in_(names)))

        rows = self.fetch_all_dict(stm)

        return rows

    def get_asteroids_by_base_dynclass(self, dynclass):

        stm = select(self.tbl.c).where(
            and_(self.tbl.c.base_dynclass == dynclass))

        rows = self.fetch_all_dict(stm)

        return rows

    def get_asteroids_by_dynclass(self, dynclass):

        stm = select(self.tbl.c).where(and_(self.tbl.c.dynclass == dynclass))

        rows = self.fetch_all_dict(stm)

        return rows

    def ccds_by_asteroid(self, asteroid_name):

        # des_exposure
        de = self.get_table('des_exposure')
        # des_ccd
        dc = self.get_table('des_ccd')
        # Des skybot position
        ds = self.get_table('des_skybotposition')
        # Skybot Position
        sp = self.get_table('skybot_position')

        # Clausula where pelo nome do objeto OBRIGATORIO.
        clause = list([sp.c.name == asteroid_name])

        columns = [dc.c.id, de.c.date_obs,
                   de.c.exptime, dc.c.path, dc.c.filename]

        stm = select(columns).\
            select_from(
            ds.join(
                sp, ds.c.position_id == sp.c.id
            ).join(
                dc, ds.c.ccd_id == dc.c.id
            ).join(
                de, ds.c.exposure_id == de.c.id
            )
        ).\
            where(and_(and_(*clause)))

        rows = self.fetch_all_dict(stm)

        return rows


class ObservationDao(Dao):
    def __init__(self):
        super(ObservationDao, self).__init__()

        self.tbl = self.get_table('des_observation')

    def delete_by_asteroid_name(self, name):

        stm = delete(self.tbl).where(and_(self.tbl.c.name == name))

        engine = self.get_db_engine()
        with engine.connect() as con:
            rows = con.execute(stm)

            return rows

    def get_observations_by_name(self, name):

        stm = select(self.tbl.c).where(and_(self.tbl.c.name == name))

        rows = self.fetch_all_dict(stm)

        return rows

class OccultationDao(Dao):
    def __init__(self):
        super(OccultationDao, self).__init__()

        self.tbl = self.get_table('tno_occultation')

    def delete_by_asteroid_name(self, name):

        stm = delete(self.tbl).where(and_(self.tbl.c.name == name))

        engine = self.get_db_engine()
        with engine.connect() as con:
            rows = con.execute(stm)

            return rows

    def delete_by_asteroid_id(self, id, start_period: str, end_period: str):

        stm = delete(self.tbl).where(
            and_(
                self.tbl.c.asteroid_id == id,
                self.tbl.c.date_time.between(start_period, end_period)
            )
        )

        engine = self.get_db_engine()
        with engine.connect() as con:
            rows = con.execute(stm)

            return rows

    def import_occultations(self, data):

        # Sql Copy com todas as colunas que vão ser importadas e o formato do csv.
        sql = "COPY %s (name, number, date_time, ra_star_candidate, dec_star_candidate, ra_target, dec_target, closest_approach, position_angle, velocity, delta, g, j, h, k, long, loc_t, off_ra, off_dec, proper_motion, ct, multiplicity_flag, e_ra, e_dec, pmra, pmdec, ra_star_deg, dec_star_deg, ra_target_deg, dec_target_deg, asteroid_id, created_at, have_path_coeff, occ_path_max_longitude, occ_path_min_longitude, occ_path_coeff, occ_path_is_nightside, occ_path_max_latitude, occ_path_min_latitude, base_dynclass, dynclass, catalog, predict_step, bsp_source, obs_source, orb_ele_source, bsp_planetary, leap_seconds, nima , job_id) FROM STDIN with (FORMAT CSV, DELIMITER '|', HEADER);" % self.tbl

        rowcount = self.import_with_copy_expert(sql, data)

        return rowcount


class OrbitTraceJobDao(Dao):
    def __init__(self):
        super(OrbitTraceJobDao, self).__init__()

        self.tbl = self.get_table('des_orbittracejob')

    def get_job_by_status(self, status:int):
        # (1, "Idle"),
        # (2, "Running"),
        # (3, "Completed"),
        # (4, "Failed"),
        # (5, "Aborted"),
        # (6, "Warning"),
        # (7, "Aborting"),
        stm = (
            select(self.tbl.c)
            .where(and_(self.tbl.c.status == status))
            .order_by(self.tbl.c.submit_time)
            .limit(1)
        )
        return self.fetch_one_dict(stm)

    def get_job_by_id(self, id:int) -> dict:

        stm = select(self.tbl.c).where(and_(self.tbl.c.id == id))

        return self.fetch_one_dict(stm)

    def get_status_id_from_string(self, status):
        labels = [
            "Idle",
            "Running",
            "Completed",
            "Failed",
            "Aborted",
            "Warning",
            "Aborting"
        ]
        return labels.index(status)+1

    def update_job(self, job):

        if isinstance(job['status'], str):
            job['status'] = self.get_status_id_from_string(job['status'])

        stm = update(self.tbl).where(and_(self.tbl.c.id == int(job['id']))).values(
            path=job['path'],
            status=job['status'],
            start=job['start'],
            end=job['end'],
            exec_time=timedelta(seconds=job['exec_time']),
            error=job['error'],
            traceback=job['traceback'],
            count_asteroids=job.get('count_asteroids', 0),
            count_ccds=job.get('count_ccds',0),
            count_observations=job.get('count_observations', 0),
            count_success=job.get('count_success', 0),
            count_failures=job.get('count_failures', 0),
            h_exec_time=job.get('h_exec_time', None),
            # condor_job_submited=job.get('condor_job_submited', 0),
            # condor_job_completed=job.get('condor_job_completed', 0),
            # condor_job_removed=job.get('condor_job_removed', 0),
            avg_exec_time_asteroid=job.get('avg_exec_time_asteroid', 0),
            avg_exec_time_ccd=job.get('avg_exec_time_ccd', 0) 
        )

        engine = self.get_db_engine()
        with engine.connect() as con:
            return con.execute(stm)


    def development_reset_job(self, job_id):
        job = self.get_job_by_id(job_id)

        stm = update(self.tbl).where(and_(self.tbl.c.id == int(job['id']))).values(
            path="",
            submit_time=datetime.now(tz=timezone.utc),
            status=1,
            start=None,
            end=None,
            exec_time=timedelta(seconds=0),
            error=None,
            traceback=None,
            count_asteroids=0,
            count_ccds=0,
            count_observations=0,
            count_success=0,
            count_failures=0,
            h_exec_time=0,
        )

        engine = self.get_db_engine()
        with engine.connect() as con:
            return con.execute(stm)

class OrbitTraceJobResultDao(Dao):
    def __init__(self):
        super(OrbitTraceJobResultDao, self).__init__()

        self.tbl = self.get_table('des_orbittracejobresult')


    def import_orbit_trace_results(self, data):

        # Sql Copy com todas as colunas que vão ser importadas e o formato do csv.
        sql = "COPY %s (name, number, base_dynclass, dynclass, status, spk_id, observations, ccds, error, asteroid_id, job_id) FROM STDIN with (FORMAT CSV, DELIMITER '|', HEADER);" % self.tbl

        rowcount = self.import_with_copy_expert(sql, data)

        return rowcount

    def delete_by_job_id(self, job_id):

        stm = delete(self.tbl).where(and_(self.tbl.c.job_id == job_id))

        engine = self.get_db_engine()
        with engine.connect() as con:
            rows = con.execute(stm)

            return rows
        
    def teste_by_id(self, id:int) -> dict:

        stm = select(self.tbl.c).where(and_(self.tbl.c.id == id))

        return self.fetch_one_dict(stm)        
    

class PredictOccultationJobDao(Dao):
    def __init__(self):
        super(PredictOccultationJobDao, self).__init__()

        self.tbl = self.get_table('tno_predictionjob')

    def get_job_by_status(self, status:int):
        # (1, "Idle"),
        # (2, "Running"),
        # (3, "Completed"),
        # (4, "Failed"),
        # (5, "Aborted"),
        # (6, "Warning"),
        # (7, "Aborting"),
        stm = (
            select(self.tbl.c)
            .where(and_(self.tbl.c.status == status))
            .order_by(self.tbl.c.submit_time)
            .limit(1)
        )
        return self.fetch_one_dict(stm)

    def get_job_by_id(self, id:int) -> dict:

        stm = select(self.tbl.c).where(and_(self.tbl.c.id == id))

        return self.fetch_one_dict(stm)

    def get_status_id_from_string(self, status):
        labels = [
            "Idle",
            "Running",
            "Completed",
            "Failed",
            "Aborted",
            "Warning",
            "Aborting"
        ]
        return labels.index(status)+1

    def update_job(self, job):

        if isinstance(job['status'], str):
            status = self.get_status_id_from_string(job['status'])

        stm = update(self.tbl).where(and_(self.tbl.c.id == int(job['id']))).values(
            status=status,
            path=job['path'],
            start=job.get('start', None),
            end=job.get('end', None),
            exec_time=timedelta(seconds=job.get('exec_time', 0)),
            # h_exec_time=job.get('h_exec_time', None),
            avg_exec_time=job.get('avg_exec_time', 0),
            count_asteroids=job.get('count_asteroids', 0),
            count_asteroids_with_occ=job.get('ast_with_occ', 0),
            count_occ=job.get('occultations', 0),
            count_success=job.get('count_success', 0),
            count_failures=job.get('count_failures', 0),
            error=job.get('error', None),
            traceback=job.get('traceback', None),            
        )

        engine = self.get_db_engine()
        with engine.connect() as con:
            return con.execute(stm)    
        
    def development_reset_job(self, job_id):
        job = self.get_job_by_id(job_id)

        stm = update(self.tbl).where(and_(self.tbl.c.id == int(job['id']))).values(
            path="",
            submit_time=datetime.now(tz=timezone.utc),
            status=1,
            start=None,
            end=None,
            exec_time=timedelta(seconds=0),
            error=None,
            traceback=None,
            count_asteroids=0,
            count_asteroids_with_occ=0,
            count_occ=0,
            count_success=0,
            count_failures=0,
            avg_exec_time=0,
        )

        engine = self.get_db_engine()
        with engine.connect() as con:
            return con.execute(stm)

class PredictOccultationJobResultDao(Dao):
    def __init__(self):
        super(PredictOccultationJobResultDao, self).__init__()

        self.tbl = self.get_table('tno_predictionjobresult')


    def import_predict_occultation_results(self, data):

        # Sql Copy com todas as colunas que vão ser importadas e o formato do csv.
        sql = "COPY %s (name, number, base_dynclass, dynclass, status, des_obs, obs_source, orb_ele_source, occultations, ing_occ_count, exec_time, messages, asteroid_id, job_id, des_obs_start, des_obs_finish, des_obs_exec_time, bsp_jpl_start, bsp_jpl_finish, bsp_jpl_dw_time, obs_start, obs_finish, obs_dw_time, orb_ele_start, orb_ele_finish, orb_ele_dw_time, ref_orb_start, ref_orb_finish, ref_orb_exec_time, pre_occ_start, pre_occ_finish, pre_occ_exec_time, ing_occ_start, ing_occ_finish, ing_occ_exec_time) FROM STDIN with (FORMAT CSV, DELIMITER '|', HEADER);" % self.tbl

        rowcount = self.import_with_copy_expert(sql, data)

        return rowcount

    def delete_by_job_id(self, job_id):

        stm = delete(self.tbl).where(and_(self.tbl.c.job_id == job_id))

        engine = self.get_db_engine()
        with engine.connect() as con:
            rows = con.execute(stm)

            return rows
        
class PredictOccultationJobStatusDao(Dao):
    def __init__(self):
        super(PredictOccultationJobStatusDao, self).__init__()

        self.tbl = self.get_table('tno_predictionjobstatus')

    def update_or_insert(self, job_id: int, step: int,
            task: str = None, 
            status: int = None,
            count: int = 0,
            current: int = 0,
            average_time: float = 0,
            time_estimate: float = 0,
            success: int = 0,
            failures: int = 0):
        
        prev = self.get_by_step(job_id, step )

        stm = insert(self.tbl)\
            .values(
                job_id=job_id,
                step=step,
                task=task,
                status=int(status),
                count=int(count),
                current=int(current),
                average_time=average_time,
                time_estimate=time_estimate,
                success=success,
                failures=failures,
                updated=datetime.now(tz=timezone.utc)
            )

        if prev is not None:
            stm = update(self.tbl)\
            .where(and_(
                self.tbl.c.job_id == job_id, 
                self.tbl.c.step == step))\
            .values(
                task=task,
                status=int(status),
                count=int(count),
                current=int(current),
                average_time=average_time,
                time_estimate=time_estimate,
                success=success,
                failures=failures,
                updated=datetime.now(tz=timezone.utc)
            )

        engine = self.get_db_engine()
        with engine.connect() as con:
            return con.execute(stm)

    def get_by_step(self, job_id: int, step: int):
        stm = select(self.tbl.c).where(and_(
            self.tbl.c.job_id == job_id,
            self.tbl.c.step == step))
        return self.fetch_one_dict(stm)


    def delete_by_job_id(self, job_id):

        stm = delete(self.tbl).where(and_(
            self.tbl.c.job_id == job_id))

        engine = self.get_db_engine()
        with engine.connect() as con:
            rows = con.execute(stm)

            return rows        
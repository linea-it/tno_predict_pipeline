import json
import logging
import pathlib
from datetime import datetime, timezone

from external_inputs.jpl import get_bsp_from_jpl
from external_inputs.mpc import (generate_resumed_orbital_elements,
                                 get_mpc_observations,
                                 get_mpc_orbital_elements)
from external_inputs.astdys import (
    get_astdys_orbital_elements, get_astdys_observations)


class AsteroidExternalInputs():

    BSP_START_PERIOD = '2012-01-01'
    BSP_YEARS_AHEAD = 1
    BSP_YEARS_BEHIND = 1
    BSP_DAYS_TO_EXPIRE = 60
    MPC_DAYS_TO_EXPIRE = 30

    # Email utilizado para baixar os BSP do JPL
    JPL_EMAIL = 'sso-portal@linea.gov.br'

    def __init__(self, name, asteroid_path, number=None, logname="refine"):

        self.name = name

        self.number = None
        if number is not None and number != '-' and number != '':
            self.number = str(number)

        self.alias = name.replace(' ', '')

        self.log = logging.getLogger(logname)

        if isinstance(asteroid_path, str):
            asteroid_path = pathlib.Path(asteroid_path)
        self.path = asteroid_path

    def get_bsp_path(self):
        filename = '{}.bsp'.format(self.alias)
        filepath = self.path.joinpath(filename)

        return filepath

    def get_orbital_elements_path(self, source):
        if source == 'AstDys':
            filename = '{}.eq0'.format(self.alias)
        else:
            filename = '{}.eqm'.format(self.alias)
        filepath = self.path.joinpath(filename)

        return filepath

    def get_observations_path(self, source):
        if source == 'AstDys':
            filename = '{}.rwo'.format(self.alias)
        else:
            filename = '{}.rwm'.format(self.alias)

        filepath = self.path.joinpath(filename)

        return filepath

    # def calculate_bsp_start_period(self, start_period):

    #     years_behind = int(self.BSP_YEARS_BEHIND)
    #     start_period = datetime.strptime(str(start_period), '%Y-%m-%d')
    #     start = datetime(year=start_period.year-years_behind, month=1, day=1)

    #     return start.strftime('%Y-%m-%d')

    # def calculate_bsp_end_period(self, end_period):

    #     years_ahead = int(self.BSP_YEARS_AHEAD)
    #     end_period = datetime.strptime(str(end_period), '%Y-%m-%d')
    #     end = datetime(year=end_period.year+years_ahead, month=12, day=31)

    #     return end.strftime('%Y-%m-%d')

    # def download_jpl_bsp(self, end_period, force=False, start_period=None):
    #     """
    #         Exemplo do retorno:
    #             {
    #                 'source': 'JPL',
    #                 'filename': '2010BJ35.bsp',
    #                 'size': 225280,
    #                 'start_period': '2012-01-01',
    #                 'end_period': '2024-12-31',
    #                 'dw_start': '2021-11-23T20:27:21.014818+00:00',
    #                 'dw_finish': '2021-11-23T20:27:23.887789+00:00',
    #                 'dw_time': 2.872971
    #             }
    #     """

    #     self.log.debug("Retriving BSP JPL started")

    #     bsp_path = self.get_bsp_path()

    #     if force is True and bsp_path.exists():
    #         # Remove o arquivo se já existir e force=True
    #         # Um novo download será realizado.
    #         bsp_path.unlink()

    #     if start_period is None:
    #         start_period = self.BSP_START_PERIOD
    #     else:
    #         start_period = self.calculate_bsp_start_period(start_period)

    #     t0 = datetime.now(tz=timezone.utc)
    #     end_period = self.calculate_bsp_end_period(end_period)

    #     try:
    #         bsp_path = get_bsp_from_jpl(
    #             self.name,
    #             start_period,
    #             end_period,
    #             self.JPL_EMAIL,
    #             self.path
    #         )

    #         t1 = datetime.now(tz=timezone.utc)
    #         tdelta = t1 - t0

    #         self.log.info(
    #             "Asteroid [%s] BSP Downloaded in %s" % (self.name, tdelta))

    #         data = dict({
    #             'source': 'JPL',
    #             'filename': bsp_path.name,
    #             'size': bsp_path.stat().st_size,
    #             'start_period': start_period,
    #             'end_period': end_period,
    #             'dw_start': t0.isoformat(),
    #             'dw_finish': t1.isoformat(),
    #             'dw_time': tdelta.total_seconds(),
    #             'downloaded_in_this_run': True
    #         })

    #         return data
    #     except Exception as e:
    #         self.log.warning("Failed to Download BSP. Error: [%s]" % e)
    #         return None

    def download_mpc_orbital_elements(self, force=False):

        self.log.debug("Retriving MPC Orbital Elements started")

        t0 = datetime.now(tz=timezone.utc)

        fpath = self.get_orbital_elements_path(source='MPC')
        if fpath.exists() and force is True:
            fpath.unlink()

        fpath = get_mpc_orbital_elements(
            name=self.name,
            number=self.number,
            output_path=self.path
        )

        if fpath is not None:

            eqm_path = generate_resumed_orbital_elements(
                name=self.name,
                input_file=fpath,
                output_path=self.path
            )
            # Remove o arquivo Json que foi baixado
            fpath.unlink()

            t1 = datetime.now(tz=timezone.utc)
            tdelta = t1 - t0

            self.log.info("Orbital Elements Downloaded in %s" % tdelta)

            data = dict({
                'source': 'MPC',
                'filename': eqm_path.name,
                'size': eqm_path.stat().st_size,
                'dw_start': t0.isoformat(),
                'dw_finish': t1.isoformat(),
                'dw_time': tdelta.total_seconds(),
                'downloaded_in_this_run': True,
            })

            return data
        else:
            return None

    def download_mpc_observations(self, force=True):

        self.log.debug("Retriving MPC Observations started")

        fpath = self.get_observations_path(source="MPC")
        if fpath.exists() and force is True:
            fpath.unlink()

        t0 = datetime.now(tz=timezone.utc)

        fpath = get_mpc_observations(
            name=self.name,
            number=self.number,
            output_path=self.path
        )

        t1 = datetime.now(tz=timezone.utc)
        tdelta = t1 - t0

        self.log.info("MPC Observations Downloaded in %s" % tdelta)

        data = dict({
            'source': 'MPC',
            'filename': fpath.name,
            'size': fpath.stat().st_size,
            'dw_start': t0.isoformat(),
            'dw_finish': t1.isoformat(),
            'dw_time': tdelta.total_seconds(),
            'downloaded_in_this_run': True
        })

        return data

    def download_astdys_orbital_elements(self, force=False):
        self.log.debug("Retriving AstDys Orbital Elements started")

        fpath = self.get_orbital_elements_path(source='AstDys')
        if fpath.exists() and force is True:
            fpath.unlink()

        t0 = datetime.now(tz=timezone.utc)

        fpath = get_astdys_orbital_elements(
            name=self.name,
            number=self.number,
            output_path=self.path
        )

        if fpath is not None:

            t1 = datetime.now(tz=timezone.utc)
            tdelta = t1 - t0

            self.log.info("Orbital Elements Downloaded in %s" % tdelta)

            data = dict({
                'source': 'AstDys',
                'filename': fpath.name,
                'size': fpath.stat().st_size,
                'dw_start': t0.isoformat(),
                'dw_finish': t1.isoformat(),
                'dw_time': tdelta.total_seconds(),
                'downloaded_in_this_run': True,
            })

            return data
        else:
            return None

    def download_astdys_observations(self, force=False):

        self.log.debug("Retriving AstDys Observations started")

        fpath = self.get_observations_path(source='AstDys')
        if fpath.exists() and force is True:
            fpath.unlink()

        t0 = datetime.now(tz=timezone.utc)

        fpath = get_astdys_observations(
            name=self.name,
            number=self.number,
            output_path=self.path
        )

        t1 = datetime.now(tz=timezone.utc)
        tdelta = t1 - t0

        self.log.info("AstDys Observations Downloaded in %s" % tdelta)
        if fpath is not None:
            data = dict({
                'source': 'AstDys',
                'filename': fpath.name,
                'size': fpath.stat().st_size,
                'dw_start': t0.isoformat(),
                'dw_finish': t1.isoformat(),
                'dw_time': tdelta.total_seconds(),
                'downloaded_in_this_run': True
            })

            return data
        else:
            return None

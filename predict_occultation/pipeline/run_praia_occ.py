#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
import traceback
import os
import argparse
from library import check_leapsec, check_bsp_planetary, check_bsp_object, clear_for_rerun
from generate_dates import generate_dates_file
from generate_ephemeris import generate_ephemeris, run_elimina, centers_positions_to_deg
from search_candidates import search_candidates
from dao import GaiaDao, MissingDBURIException
from datetime import datetime
import sys

parser = argparse.ArgumentParser()
parser.add_argument("name", help="Object name without spaces")
parser.add_argument("start_date", help="Initial date. example '2018-JAN-01'")
parser.add_argument(
    "final_date", help="Final date. example '2018-DEC-31 23:59:01'")
parser.add_argument("step",
                    help="steps in seconds. Example 60")
parser.add_argument("--leap_sec", default="naif0012.tls",
                    help="Name of the Leap Seconds file, it must be in the directory /data. example naif0012.tls")
parser.add_argument("--bsp_planetary", default="de435.bsp",
                    help="Name of the BSP Planetary file, it must be in the directory /data. example de435.bsp")
parser.add_argument("--bsp_object", default=None,
                    help="Name of the Asteroid BSP file, it must be in the directory /data. example Eris.bsp. default <name>.bsp")
parser.add_argument("-p", "--path", default=None,
                    required=False,
                    help="Path where the inputs are and where the outputs will be. must be the path as it is mounted on the volume, should be used when it is not possible to mount the volume as /data. example the inputs are in /archive/asteroids/Eris and this path is mounted inside the container the parameter --path must have this value --path /archive/asteroids/Eris, the program will create a link from this path to /data.")


def start_praia_occ(
        name, start_date, final_date, step,
        leap_sec_filename, bsp_planetary_filename,
        bsp_object_filename):

    log_file = os.path.join(os.environ.get("DIR_DATA"), "praia_occ.log")

    orig_stdout = sys.stdout
    f = open(log_file, 'w')
    sys.stdout = f

    # Seta os nomes de arquivos que serão gerados.
    dates_filename = "dates.txt"
    eph_filename = "%s.eph" % name
    radec_filename = "radec.txt"
    positions_filename = "positions.txt"
    centers_filename = "centers.txt"
    centers_deg_filename = "centers_deg.csv"
    gaia_cat_filename = "gaia_catalog.cat"
    gaia_csv_filename = "gaia_catalog.csv"
    occultation_table_filename = 'occultation_table.csv'
    if bsp_object_filename is None:
        bsp_object_filename = "%s.bsp" % name

    # Inputs/Outputs do PRAIA Occ Star Search,
    # IMPORTANTE! esses filenames são HARDCODED na função praia_occ_input_file
    search_input_filename = "praia_occ_star_search_12.dat"
    stars_catalog_mini_filename = 'g4_micro_catalog_JOHNSTON_2018'
    stars_catalog_xy_filename = 'g4_occ_catalog_JOHNSTON_2018'
    stars_parameters_of_occultation_filename = 'g4_occ_data_JOHNSTON_2018'
    stars_parameters_of_occultation_plot_filename = 'g4_occ_data_JOHNSTON_2018_table'
    praia_occ_log_filename = "praia_star_search.log"

    # Limpa o diretório app e data removendo os links simbolicos e resultados
    # Util quando se roda varias vezes o mesmo job.
    clear_for_rerun(
        input_files=[bsp_object_filename, gaia_cat_filename],
        # input_files=[gaia_cat_filename],
        output_files=[
            dates_filename, eph_filename, radec_filename,
            positions_filename, centers_filename,
            centers_deg_filename, gaia_csv_filename,
            search_input_filename, stars_catalog_mini_filename,
            stars_catalog_xy_filename, stars_parameters_of_occultation_filename,
            stars_parameters_of_occultation_plot_filename, occultation_table_filename,
            praia_occ_log_filename
        ])

    # Checar o arquivo de leapserconds
    leap_sec = check_leapsec(leap_sec_filename)
    print("Leap Second: [%s]" % leap_sec_filename)

    # Checa o arquivo bsp_planetary
    bsp_planetary = check_bsp_planetary(bsp_planetary_filename)
    print("BSP Planetary: [%s]" % bsp_planetary)

    # Checa o arquivo bsp_object
    bsp_object = check_bsp_object(bsp_object_filename)
    print("BSP Object: [%s]" % bsp_object)

    # Gerar arquivo de datas
    dates_file = generate_dates_file(
        start_date, final_date, step, dates_filename)
    print("Dates File: [%s]" % dates_file)

    # Gerar a ephemeris
    eph_file = generate_ephemeris(
        dates_file, bsp_object, bsp_planetary,
        leap_sec, eph_filename, radec_filename)

    print("Ephemeris File: [%s]" % eph_file)

    # # TODO: Verificar se é mesmo necessário!
    # # Gerar aquivo de posições
    # positions_file = generate_positions(
    #     eph_filename, positions_filename)

    # print("Positions File: [%s]" % positions_file)

    # TODO: Gerar plot Orbit in Sky se for necessário
    # plotOrbit(object_name, footprint, ecliptic_galactic,
    #         positions, orbit_in_sky)
    # os.chmod(orbit_in_sky, 0776)

    # Executar o Elimina e gerar o Centers.txt
    centers_file = run_elimina(eph_filename, centers_filename)
    print("Centers File: [%s]" % centers_file)

    # Converter as posições do Centers.txt para graus
    # gera um arquivo com as posições convertidas, mas o retorno da função é um array.
    center_positions = centers_positions_to_deg(
        centers_file, centers_deg_filename)

    # Para cada posição executa a query no banco de dados.
    dao = GaiaDao()
    df_catalog = dao.catalog_by_positions(center_positions, radius=0.15)
    # Cria um arquivo no formato especifico do praia_occ
    gaia_cat = dao.write_gaia_catalog(
        df_catalog.to_dict('records'),
        gaia_cat_filename)

    print("Gaia Cat: [%s]" % gaia_cat)

    # Cria um arquivo csv do catalogo gaia.
    gaia_csv = dao.gaia_catalog_to_csv(df_catalog, gaia_csv_filename)

    print("Gaia CSV: [%s]" % gaia_csv)

    # Run PRAIA OCC Star Search 12
    # Criar arquivo .dat baseado no template.
    occultation_file = search_candidates(
        star_catalog=gaia_cat,
        object_ephemeris=eph_file,
        filename=occultation_table_filename
    )

    print("Occultation CSV Table: [%s]" % occultation_file)

    sys.stdout = orig_stdout
    f.close()

    os.chmod(log_file, 0664)

    return occultation_file


if __name__ == "__main__":

    args = parser.parse_args()

    t0 = datetime.now()

    # Verifica o path onde o programa está sendo executado
    app_path = os.environ.get("APP_PATH")
    original_cwd = os.getcwd()
    print("Current Path: %s" % original_cwd)

    if original_cwd != app_path:
        print("Changing the work directory")
        os.chdir(app_path)
        print("Current Path: %s" % os.getcwd())

    try:

        # Verifica Variavel de Ambiente com a URI de acesso ao banco de dados.
        try:
            db_uri = os.environ['DB_URI']
        except Exception as e:
            raise MissingDBURIException(
                "Required environment variable with URI to access the database where GAIA DR2 is."
                "example DB_URI=postgresql+psycopg2://USER:PASS@HOST:PORT/DB_NAME")

        # Tratar o diretório dos inputs
        if args.path is not None and os.path.exists(os.environ.get("DIR_DATA")) is False:
            # Se for passado o parametro --path e o diretório /tmp/data nao existir
            # Cria um link simbolico do --path para /tmp/data (não pode ser /data por causa de permissão.)
            # Altera a variavel de ambiente DIR_DATA com o valor /tmp/data.
            # é necessário criar este link por que os paths para os arquivos
            # não podem ser muito grande limite de 50 caracteres para o PRAIA_OCC.
            os.symlink(args.path, os.environ.get("DIR_DATA"))

        if args.path is None and os.path.exists(os.environ.get("DIR_DATA")) is False:
            # Se não for passado o parametro --path e o diretório /data não existir o programa para a execução.
            raise Exception(
                "No data directory was found. use the volume mounting the data in the /data directory or run the run.py script with parameter --path in which case the directory passed as parameter must be a mounted volume.")

        # Diretorio de Dados dentro do container.
        data_dir = os.environ.get("DIR_DATA").rstrip('/')
        print("DATA DIR: [%s]" % data_dir)

        # Tratar os Parametros de entrada
        name = args.name.replace(' ', '').replace('_', '')
        start_date = args.start_date
        final_date = args.final_date
        step = args.step
        leap_sec_filename = args.leap_sec
        bsp_planetary_filename = args.bsp_planetary
        bsp_object_filename = args.bsp_object

        occultation_file = start_praia_occ(
            name, start_date, final_date, step,
            leap_sec_filename, bsp_planetary_filename,
            bsp_object_filename)

        # if bsp_object_filename is None:
        #     bsp_object_filename = "%s.bsp" % name

        # # Seta os nomes de arquivos que serão gerados.
        # dates_filename = "dates.txt"
        # eph_filename = "%s.eph" % name
        # radec_filename = "radec.txt"
        # positions_filename = "positions.txt"
        # centers_filename = "centers.txt"
        # centers_deg_filename = "centers_deg.csv"
        # gaia_cat_filename = "gaia_catalog.cat"
        # gaia_csv_filename = "gaia_catalog.csv"
        # occultation_table_filename = 'occultation_table.csv'

        # # Inputs/Outputs do PRAIA Occ Star Search,
        # # IMPORTANTE! esses filenames são HARDCODED na função praia_occ_input_file
        # search_input_filename = "praia_occ_star_search_12.dat"
        # stars_catalog_mini_filename = 'g4_micro_catalog_JOHNSTON_2018'
        # stars_catalog_xy_filename = 'g4_occ_catalog_JOHNSTON_2018'
        # stars_parameters_of_occultation_filename = 'g4_occ_data_JOHNSTON_2018'
        # stars_parameters_of_occultation_plot_filename = 'g4_occ_data_JOHNSTON_2018_table'
        # praia_occ_log_filename = "praia_star_search.log"

        # # Limpa o diretório app e data removendo os links simbolicos e resultados
        # # Util quando se roda varias vezes o mesmo job.
        # clear_for_rerun(
        #     input_files=[bsp_object_filename],
        #     output_files=[
        #         dates_filename, eph_filename, radec_filename,
        #         positions_filename, centers_filename,
        #         centers_deg_filename, gaia_cat_filename, gaia_csv_filename,
        #         search_input_filename, stars_catalog_mini_filename,
        #         stars_catalog_xy_filename, stars_parameters_of_occultation_filename,
        #         stars_parameters_of_occultation_plot_filename, occultation_table_filename,
        #         praia_occ_log_filename
        #     ])

        # # Checar o arquivo de leapserconds
        # leap_sec = check_leapsec(leap_sec_filename)
        # print("Leap Second: [%s]" % leap_sec_filename)

        # # Checa o arquivo bsp_planetary
        # bsp_planetary = check_bsp_planetary(bsp_planetary_filename)
        # print("BSP Planetary: [%s]" % bsp_planetary)

        # # Checa o arquivo bsp_object
        # bsp_object = check_bsp_object(bsp_object_filename)
        # print("BSP Object: [%s]" % bsp_object)

        # # Gerar arquivo de datas
        # dates_file = generate_dates_file(
        #     start_date, final_date, step, dates_filename)
        # print("Dates File: [%s]" % dates_file)

        # # Gerar a ephemeris
        # eph_file = generate_ephemeris(
        #     dates_file, bsp_object, bsp_planetary,
        #     leap_sec, eph_filename, radec_filename)

        # print("Ephemeris File: [%s]" % eph_file)

        # # # TODO: Verificar se é mesmo necessário!
        # # # Gerar aquivo de posições
        # # positions_file = generate_positions(
        # #     eph_filename, positions_filename)

        # # print("Positions File: [%s]" % positions_file)

        # # TODO: Gerar plot Orbit in Sky se for necessário
        # # plotOrbit(object_name, footprint, ecliptic_galactic,
        # #         positions, orbit_in_sky)
        # # os.chmod(orbit_in_sky, 0776)

        # # Executar o Elimina e gerar o Centers.txt
        # centers_file = run_elimina(eph_filename, centers_filename)
        # print("Centers File: [%s]" % centers_file)

        # # Converter as posições do Centers.txt para graus
        # # gera um arquivo com as posições convertidas, mas o retorno da função é um array.
        # center_positions = centers_positions_to_deg(
        #     centers_file, centers_deg_filename)

        # # Para cada posição executa a query no banco de dados.
        # dao = GaiaDao()
        # df_catalog = dao.catalog_by_positions(center_positions, radius=0.15)
        # # Cria um arquivo no formato especifico do praia_occ
        # gaia_cat = dao.write_gaia_catalog(
        #     df_catalog.to_dict('records'),
        #     gaia_cat_filename)

        # print("Gaia Cat: [%s]" % gaia_cat)

        # # Cria um arquivo csv do catalogo gaia.
        # gaia_csv = dao.gaia_catalog_to_csv(df_catalog, gaia_csv_filename)

        # print("Gaia CSV: [%s]" % gaia_csv)

        # # Run PRAIA OCC Star Search 12
        # # Criar arquivo .dat baseado no template.
        # occultation_file = search_candidates(
        #     star_catalog=gaia_cat,
        #     object_ephemeris=eph_file,
        #     filename=occultation_table_filename
        # )

        # print("Occultation CSV Table: [%s]" % occultation_file)

    except Exception as e:
        print(e)
        traceback.print_exc()

    except MissingDBURIException as e:
        print(e)

    finally:

        # Volta para o diretório original
        os.chdir(original_cwd)

        t1 = datetime.now()
        td = t1 - t0
        print("Predict Occultation Done in %s" % td)

# Exemplo da execução do comando
# python run.py 1999RB216 2021-JAN-01 2022-JAN-01 600 --bsp_object 1999RB216.bsp
# docker run -it --rm --volume /home/glauber/linea/1999RB216:/data --volume $PWD:/app --network host -e DB_URI=postgresql+psycopg2://postgres:postgres@172.18.0.2:5432/tno_v2 linea/praiaoccultation:v2.0 bash
# docker run -it --rm --volume /home/glauber/linea/1999RB216:/data --volume $PWD:/app --network host -e DB_URI=postgresql+psycopg2://postgres:postgres@172.18.0.2:5432/tno_v2 linea/praiaoccultation:v2.0 python run.py 1999RB216 2021-JAN-01 2022-JAN-01 600 --bsp_object 1999RB216.bsp
# docker run -it --rm --volume /home/glauber/linea/1999RB216:/home/glauber/linea/1999RB216 --volume $PWD:/app --network host -e DB_URI=postgresql+psycopg2://postgres:postgres@172.18.0.2:5432/tno_v2 linea/praiaoccultation:v2.0 python run.py 1999RB216 2021-JAN-01 2022-JAN-01 600 --bsp_object 1999RB216.bsp --path /home/glauber/linea/1999RB216


# docker run -it --rm --volume /home/glauber/linea/1999RB216:/home/glauber/linea/1999RB216 --network host -e DB_URI=postgresql+psycopg2://postgres:postgres@172.18.0.2:5432/tno_v2 linea/praiaoccultation:v2.5 python run.py 1999RB216 2021-JAN-01 2022-JAN-01 600 --bsp_object 1999RB216.bsp --path /home/glauber/linea/1999RB216

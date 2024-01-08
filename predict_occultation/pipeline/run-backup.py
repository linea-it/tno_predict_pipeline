#!/usr/bin/python2.7
# -*- coding: utf-8 -*-

import argparse
import os
import traceback
import os
import argparse
from datetime import datetime, timedelta
from run_nima import start_nima
from run_praia_occ import start_praia_occ
from library import read_asteroid_json, write_asteroid_json, count_lines, create_nima_input

parser = argparse.ArgumentParser()
parser.add_argument(
    "name", help="Object name without spaces. example '1999RB216'")
parser.add_argument("start_date", help="Initial date. example '2018-01-01'")
parser.add_argument(
    "final_date", help="Final date. example '2018-12-31'")
parser.add_argument("--number", default=None,
                    help="Asteroid number. if not informed, the name will be used. example '137295'")
parser.add_argument("--step",
                    help="steps in seconds. Example 60", default=600)
parser.add_argument("--leap_sec", default="naif0012.tls",
                    help="Name of the Leap Seconds file, it must be in the directory /data. example naif0012.tls")
parser.add_argument("--bsp_planetary", default="de440.bsp",
                    help="Name of the BSP Planetary file, it must be in the directory /data. example de440.bsp")
parser.add_argument("--bsp_object", default=None,
                    help="Name of the Asteroid BSP file, it must be in the directory /data. example Eris.bsp. default <name>.bsp")
parser.add_argument("-p", "--path", default=None,
                    required=False,
                    help="Path where the inputs are and where the outputs will be. must be the path as it is mounted on the volume, should be used when it is not possible to mount the volume as /data. example the inputs are in /archive/asteroids/Eris and this path is mounted inside the container the parameter --path must have this value --path /archive/asteroids/Eris, the program will create a link from this path to /data.")
parser.add_argument("-c", "--callback_path", default=None,
                    required=False,
                    help="Directory where a copy of asteroid.json will be placed at the end of execution. Useful for communicating the end of the task to other programs.")

if __name__ == "__main__":
    t0 = datetime.now()

    args = parser.parse_args()

    # Verifica o path onde o programa está sendo executado
    app_path = os.environ.get("APP_PATH")
    original_cwd = os.getcwd()
    print("Current Path: %s" % original_cwd)

    if original_cwd != app_path:
        print("Changing the work directory")
        os.chdir(app_path)
        print("Current Path: %s" % os.getcwd())

    try:

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

        if args.callback_path is not None and os.path.exists(args.callback_path) is False:
            raise Exception(
                "Callbacks directory not found. The directory passed as parameter must be a mounted volume.")            

        # Diretorio de Dados dentro do container.
        data_dir = os.environ.get("DIR_DATA").rstrip('/')
        print("DATA DIR: [%s]" % data_dir)

        # Diretório de Callback
        callback_path = args.callback_path

        # Tratar os Parametros de entrada
        name = args.name.replace(' ', '').replace('_', '')
        number = args.number
        # Converter para datetime para testar se a data é valida
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        final_date = datetime.strptime(args.final_date, '%Y-%m-%d').date()
        step = args.step
        leap_sec_filename = args.leap_sec
        bsp_planetary_filename = args.bsp_planetary
        bsp_object_filename = args.bsp_object
        des_positions_filename = "%s.txt" % name

        # Até aqui preparou o ambiente para a execução do Nima e Praia OCC em sequencia.

        # Procura por um arquivo json com os dados do objeto
        # Se o arquivo exisitir os dados da execução das etapas sera escrito nele.
        # Se não existir sera um dicionario vazio onde serão colocados esses dados e depois salvo como asteroidename.json
        obj_data = read_asteroid_json(name)

        # VERIFICA SE O Objeto tem posições do DES
        # se não tiver a etapa do NIMA deve ser ignorada
        # e a predição feita com o BSP do JPL
        des_positions = os.path.join(data_dir, des_positions_filename)
        if os.path.exists(des_positions) and os.path.getsize(des_positions) > 0:
            print("Running NIMAv7")

            nima_t0 = datetime.now()

            # Criar o arquivo de inputs do NIMA
            # A ephemeris gerada pelo NIMA precisa ter um periodo um pouco maior que necessário para a predição
            # para evitar erros no Praia OCC pq durante a geração das ephemeris pode gerar uma posição alguns segundos depois do final do periodo.
            # Acrescenta 30 dias a mais ao periodo,
            period_end = final_date
            period_end += timedelta(days=30)

            nima_input = create_nima_input(name, number, period_end)
            print("Nima input file: [%s]" % nima_input)

            # Executar o NIMA
            files = start_nima()

            # Depois de executar o path, é necessário voltar para o path /app
            os.chdir(app_path)
            print("NIMAv7 is Done!")

            bsp_nima_filename = "%s_nima.bsp" % name
            bsp_nima = os.path.join(data_dir, bsp_nima_filename)
            if not os.path.exists(bsp_nima):
                # Se o arquivo bsp nima não for criado interronpe a execução do programa.
                raise Exception("Failed to create BSP NIMA. [%s]" % bsp_nima)

            # Será usado o BSP gerado pelo NIMA.
            bsp_object_filename = bsp_nima_filename
            print("Using BSP generated by NIMA.")

            nima_t1 = datetime.now()
            nima_td = nima_t1 - nima_t0

            nima_result = dict({
                "filename": bsp_object_filename,
                "size": os.path.getsize(bsp_nima),
                "start_period": str(start_date),
                "end_period": str(period_end),
                "start": nima_t0.isoformat(),
                "finish": nima_t1.isoformat(),
                "exec_time": nima_td.total_seconds()
            })

        else:
            msg = "NIMA execution was skipped as there are no positions in DES for this object."
            print(msg)
            nima_result = dict({
                "message": msg
            })

        obj_data['refine_orbit'] = nima_result

        # ============== Executar o PRAIA OCC ==============
        praia_t0 = datetime.now()

        print("Running PRAIA OCC")
        occultation_file = start_praia_occ(
            name, str(start_date), str(final_date), step,
            leap_sec_filename, bsp_planetary_filename, bsp_object_filename
        )

        praia_t1 = datetime.now()
        praia_td = praia_t1 - praia_t0

        print("PRAIA OCC is Done!")
        if os.path.exists(occultation_file):
            print("Occultation table created: [%s]" % occultation_file)

            # Quantidade de predições.
            # Subtrai 1 por que o arquivo mesmo vazio tem os Headers
            count = count_lines(occultation_file) - 1

            praia_result = dict({
                "filename": os.path.basename(occultation_file),
                "size": os.path.getsize(occultation_file),
                "count": count,
                "start_period": str(start_date),
                "end_period": str(final_date),
                "start": praia_t0.isoformat(),
                "finish": praia_t1.isoformat(),
                "exec_time": praia_td.total_seconds()
            })

        else:
            praia_result = dict({
                "message": "PRAIA was executed but failed to create Occultation table."
            })

            raise Exception(
                "Failed to create Occultation table. [%s]" % occultation_file)

        obj_data['predict_occultation'] = praia_result

        # Escreve os dados da execução no arquivo json do objeto.
        write_asteroid_json(name, obj_data, callback_path)

        

    except Exception as e:
        print(e)
        traceback.print_exc()

    finally:

        # Volta para o diretório original
        os.chdir(original_cwd)

        t1 = datetime.now()
        td = t1 - t0
        print("Predict Occultation Done in %s" % td)


# Exemplo usando o script Run.py
# docker run -it --rm --user 10139:10000 --volume /home/glauber/linea/1999RB216:/home/glauber/linea/1999RB216 --volume /home/glauber/linea/praia_occultation/src/run.py:/app/run.py --volume /home/glauber/linea/praia_occultation/src/library.py:/app/library.py --network host -e DB_URI=postgresql+psycopg2://postgres:postgres@172.18.0.2:5432/tno_v2 linea/praiaoccultation:v2.8.2 python run.py 1999RB216 2021-10-01 2023-01-01 --number 137295 --step 600 --path /home/glauber/linea/1999RB216

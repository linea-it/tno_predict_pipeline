#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
import os


def check_leapsec(filename):
    """
        Verifica se o arquivo leapSec existe
    """
    app_path = os.environ.get("APP_PATH").rstrip('/')
    data_dir = os.environ.get("DIR_DATA").rstrip('/')

    # local_leap_sec = os.path.join(app_path, filename)
    in_leap_sec = os.path.join(data_dir, filename)

    dest = os.path.join(app_path, filename)

    # Verifica se o Arquivo existe no diretorio local
    if os.path.exists(dest):
        return filename

    else:
        # Verifica se o arquivo existe no diretório /Data
        if os.path.exists(in_leap_sec):
            # Cria um link simbolico no Diretório do app
            os.symlink(in_leap_sec, dest)
            return filename
        else:
            raise (Exception("Leap Sec %s file does not exist." % filename))


def check_bsp_planetary(filename):
    """
        Verifica se o arquivo BSP Planetary existe
    """
    app_path = os.environ.get("APP_PATH").rstrip('/')
    data_dir = os.environ.get("DIR_DATA").rstrip('/')

    in_bsp = os.path.join(data_dir, filename)

    dest = os.path.join(app_path, filename)

    # Verifica se o Arquivo existe no diretorio local
    if os.path.exists(dest):
        return filename
    else:
        # Verifica se o arquivo existe no diretório /Data
        if os.path.exists(in_bsp):
            # Cria um link simbolico no Diretório do app
            os.symlink(in_bsp, dest)
            return filename
        else:
            raise (Exception("BSP Planetary %s file does not exist." % filename))


def check_bsp_object(filename):
    """
        Verifica se o arquivo BSP Object existe e cria um link no diretório app
    """
    app_path = os.environ.get("APP_PATH").rstrip('/')
    data_dir = os.environ.get("DIR_DATA").rstrip('/')

    in_bsp = os.path.join(data_dir, filename)

    dest = os.path.join(app_path, filename)

    print('IN BSP: ', in_bsp)
    print('DEST: ', dest)

    # Verifica se o Arquivo existe no diretorio data
    if os.path.exists(in_bsp):
        # Cria um link simbolico no Diretório do app
        os.symlink(in_bsp, dest)
        return filename
    else:
        raise (Exception("BSP Object %s file does not exist. %s" % (filename, in_bsp)))


def HMS2deg(ra='', dec=''):
    RA, DEC, rs, ds = '', '', 1, 1
    if dec:
        D, M, S = [float(i) for i in dec.split()]
        if str(D)[0] == '-':
            ds, D = -1, abs(D)
        deg = D + (M/60) + (S/3600)
        DEC = deg*ds

    if ra:
        H, M, S = [float(i) for i in ra.split()]
        if str(H)[0] == '-':
            rs, H = -1, abs(H)
        deg = (H*15) + (M/4) + (S/240)
        RA = deg*rs

    if ra and dec:
        return [RA, DEC]
    else:
        return RA or DEC


def clear_for_rerun(input_files, output_files):
    """Remove os arquivos de input e output utilizados no processo. 
    - para arquivos de input remove só os links simbolicos em /app
    - para arquivos de output remove os links em /app e os arquivos originais em /data

    Args:
        input_files (list): Lista com os nomes de arquivos a serem removidos
        output_files (list): Lista com os nomes de arquivos a serem removidos
    """

    app_path = os.environ.get("APP_PATH").rstrip('/')
    data_dir = os.environ.get("DIR_DATA").rstrip('/')

    # Remover o arquivo bsp_object apenas o link
    for filename in input_files:
        a = os.path.join(app_path, filename)
        if os.path.exists(a):
            os.unlink(a)

    # Para os demais arquivos que tem link no diretório app e o arquivo no diretório data
    for filename in output_files:
        a = os.path.join(app_path, filename)
        d = os.path.join(data_dir, filename)
        if os.path.exists(a):
            os.unlink(a)
        if os.path.exists(d):
            os.remove(d)


def read_asteroid_json(asteroid_name):
    import json
    import os

    path = os.environ.get("DIR_DATA").rstrip('/')
    alias = asteroid_name.replace(' ', '').replace('_', '')
    filename = "{}.json".format(alias)

    filepath = os.path.join(path, filename)

    if os.path.exists(filepath):
        with open(filepath) as json_file:
            data = json.load(json_file)
            return data
    else:
        return dict({})


def write_asteroid_json(asteroid_name, data, callback_path=None):
    import os
    import json

    path = os.environ.get("DIR_DATA").rstrip('/')
    alias = asteroid_name.replace(' ', '').replace('_', '')
    filename = "{}.json".format(alias)

    filepath = os.path.join(path, filename)

    with open(filepath, 'w') as json_file:
        json.dump(data, json_file)

    if callback_path is not None:
        filepath = os.path.join(callback_path, filename)
        with open(filepath, 'w') as json_file:
            json.dump(data, json_file)


def count_lines(filepath):
    with open(filepath, 'r') as fp:
        num_lines = sum(1 for line in fp if line.rstrip())
        return num_lines


def create_nima_input(name, number, period_end):

    import os
    from datetime import datetime, timedelta

    path = os.environ.get("DIR_DATA").rstrip('/')
    nima_input_file = os.path.join(path, "nima_input.txt")

    # Path para arquivo template de input do NIMA
    app_path = os.environ.get("APP_PATH")
    template_file = os.path.join(app_path, "nima_input_template.txt")

    with open(template_file) as file:
        data = file.read()

        # Substitui no template as tags {} pelo valor das variaveis.
        # Parametro Asteroid Name
        name = name.replace('_', '').replace(' ', '')
        data = data.replace('{name}', name.ljust(66))

        data = data.replace('{dir_data}', path.ljust(66))

        # Parametro Asteroid Number
        if number is None or number == '-':
            number = name
        data = data.replace('{number}', number.ljust(66))

        # Parametro Plot start e Plot end
        # data = data.replace('{plot_start_date}', period_start.ljust(66))
        # year = int(period_end.split('-')[0]) - 1
        # data = data.replace('{plot_end_year}', str(year))
        data = data.replace('{plot_end}', str(period_end).ljust(66))

        # Parametro BSP start e BSP end
        # data = data.replace('{bsp_start_date}', period_start.ljust(66))
        # year = int(period_end.split('-')[0]) - 1
        # data = data.replace('{bsp_end_year}', str(year))
        data = data.replace('{bsp_end}', str(period_end).ljust(66))

        # Parametro Ephem start e Ephem end
        # data = data.replace('{ephem_start_date}', period_start.ljust(66))
        # year = int(period_end.split('-')[0]) - 1
        # data = data.replace('{ephem_end_year}', str(year))
        data = data.replace('{ephem_end}', str(period_end).ljust(66))

        with open(nima_input_file, 'w') as new_file:
            new_file.write(data)

        return nima_input_file

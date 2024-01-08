import pathlib
import shutil
from datetime import datetime as dt
import json
import requests
import spiceypy as spice
import base64


def get_bsp_from_jpl(identifier, initial_date, final_date, directory, filename):
    """Download bsp files from JPL database

        Bsp files, which have information to generate
        the ephemeris of the objects, will be downloaded
        The files will be named as (without spaces): [identifier].bsp

        Important:
            it is able to download bsp files of neither planets nor satellites

    Args:
        identifier (str): Identifier of the object.
            It can be the name, number or SPK ID.
            It can also be a list of objects.
            Examples:
                '2137295'
                '1999 RB216'
                '137295'
                'Chariklo'
        initial_date (str): Date the bsp file is to begin, within span [1900-2100].
            Examples:
                '2003-02-01'
        final_date (str): Date the bsp file is to end, within span [1900-2100].
            Must be more than 32 days later than [initial_date].
            Examples:
                '2006-01-12'
        directory (str): Directory path to save the bsp files.

    Returns:
        Path: file path for .bsp file.
    """

    date1 = dt.strptime(initial_date, '%Y-%m-%d')
    date2 = dt.strptime(final_date, '%Y-%m-%d')
    diff = date2 - date1

    if diff.days <= 32:
        raise ValueError(
            'The [final_date] must be more than 32 days later than [initial_date]')

    path = pathlib.Path(directory)
    if not path.exists():
        raise ValueError('The directory {} does not exist!'.format(path))

    spk_file = path.joinpath(filename)

    # https://ssd.jpl.nasa.gov/api/horizons.api?
    # format=text
    # &EPHEM_TYPE=SPK
    # &COMMAND=chiron
    # &START_TIME=2023-Jan-01
    # &STOP_TIME=2023-Mar-30
    urlJPL = 'https://ssd.jpl.nasa.gov/api/horizons.api'
    parameters = {
        'format': "json",
        'EPHEM_TYPE': "SPK",
        'OBJ_DATA': "YES",
        'COMMAND': identifier,
        'START_TIME': date1.strftime('%Y-%b-%d'),
        'STOP_TIME': date2.strftime('%Y-%b-%d'),
    }
    r = requests.get(urlJPL, params=parameters, stream=True)
    if r.status_code == requests.codes.ok and r.headers['Content-Type'] == 'application/json':
        try:
            data = json.loads(r.text)
            # If the SPK file was generated, decode it and write it to the output file:
            if "spk" in data:
                with open(spk_file, "wb") as f:
                    # Decode and write the binary SPK file content:
                    f.write(base64.b64decode(data["spk"]))
                    f.close()

                return spk_file 
            else:
                # Otherwise, the SPK file was not generated so output an error:
                raise Exception(f"SPK file not generated: {response.text}")
        except ValueError as e:
            raise Exception(f"Unable to decode JSON. {e}")
        except OSError as e:
            raise Exception(f"Unable to create file '{spk_file}': {e}")
        except Exception as e:
            raise(e)
    elif r.status_code == 400:
        raise Exception("Bad Request code 400 - {r.text}")
    else:
        raise Exception(
            f"It was not able to download the bsp file for object. Error: {r.status_code}")

def findSPKID(bsp):
    """Search the spk id of a small Solar System object from bsp file

    Args:
        bsp (str): File path for bsp jpl file.

    Returns:
        str: Spk id of Object or None
    """

    spice.furnsh([bsp])
    kind = 'spk'

    fillen = 256
    typlen = 33
    srclen = 256
    keys = ['Target SPK ID   :', 'ASTEROID_SPK_ID =']
    n = len(keys[0])

    name, kind, source, loc = spice.kdata(0, kind, fillen, typlen, srclen)
    flag = False
    spk = ''
    while not flag:
        try:
            m, header, flag = spice.dafec(loc, 1)
            row = header[0]
            if row[:n] in keys:
                spk = row[n:].strip()
                break
        except:
            break

    spice.kclear()

    if spk == '':
        return None

    return str(spk)

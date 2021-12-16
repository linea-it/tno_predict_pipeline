import pathlib
import shutil
from datetime import datetime as dt

import requests
import spiceypy as spice


def get_bsp_from_jpl(identifier, initial_date, final_date, email, directory):
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
                '2003-3-5'
        final_date (str): Date the bsp file is to end, within span [1900-2100].
            Must be more than 32 days later than [initial_date].
            Examples:
                '2006-01-12'
                '2006-1-12'
        email (str): User's e-mail contact address.
            Example: username@user.domain.name
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

    urlJPL = 'https://ssd.jpl.nasa.gov/x/smb_spk.cgi?OPTION=Make+SPK'

    path = pathlib.Path(directory)
    if not path.exists():
        raise ValueError('The directory {} does not exist!'.format(path))

    filename = identifier.replace(' ', '') + '.bsp'

    parameters = {
        'OBJECT': identifier,
        'START': date1.strftime('%Y-%b-%d'),
        'STOP': date2.strftime('%Y-%b-%d'),
        'EMAIL': email,
        'TYPE': '-B'
    }

    r = requests.get(urlJPL, params=parameters, stream=True)
    bspFormat = r.headers['Content-Type']
    if r.status_code == requests.codes.ok and bspFormat == 'application/download':
        filepath = path.joinpath(filename)
        with open(filepath, 'wb') as f:
            r.raw.decode_content = True
            shutil.copyfileobj(r.raw, f)

        return filepath
    else:
        # TODO: Add a Debug
        raise Exception(
            "It was not able to download the bsp file for object.")


def findSPKID(bsp):
    """Search the spk id of a small Solar System object from bsp file

    Args:
        bsp (str): File path for bsp jpl file.

    Returns:
        str: Spk id of Object
    """

    bsp = [bsp]
    spice.furnsh(bsp)

    i = 0
    kind = 'spk'
    fillen = 256
    typlen = 33
    srclen = 256
    keys = ['Target SPK ID   :', 'ASTEROID_SPK_ID =']
    n = len(keys[0])

    name, kind, source, loc = spice.kdata(i, kind, fillen, typlen, srclen)
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
    return spk

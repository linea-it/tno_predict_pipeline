#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os

from parsl import python_app


def findSPKID(bsp):
    """Search the spk id of a small Solar System object from bsp file

    Args:
        bsp (str): File path for bsp jpl file.

    Returns:
        str: Spk id of Object
    """
    import spiceypy as spice

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


def geo_topo_vector(longitude, latitude, elevation, jd):
    '''
    Transformation from [longitude, latitude, elevation] to [x,y,z]
    '''
    from astropy.coordinates import GCRS, EarthLocation
    from astropy.time import Time
    import numpy as np

    loc = EarthLocation(longitude, latitude, elevation)

    time = Time(jd, scale='utc', format='jd')
    itrs = loc.get_itrs(obstime=time)
    gcrs = itrs.transform_to(GCRS(obstime=time))

    r = gcrs.cartesian

    # convert from m to km
    x = r.x.value/1000.0
    y = r.y.value/1000.0
    z = r.z.value/1000.0

    return np.array([x, y, z])


def compute_theoretical_positions(spkid, ccds, bsp_jpl, bsp_planetary, leap_second, location):

    import spiceypy as spice
    import numpy as np
    from orbit_trace_library import geo_topo_vector

    # TODO: Provavelmente esta etapa é que causa a lentidão desta operação
    # Por que carrega o arquivo de ephemeris planetarias que é pesado
    # Load the asteroid and planetary ephemeris and the leap second (in order)
    spice.furnsh(bsp_planetary)
    spice.furnsh(leap_second)
    spice.furnsh(bsp_jpl)

    results = list()

    for ccd in ccds:
        date_jd = ccd['date_jd']

        # Convert dates from JD to et format. "JD" is added due to spice requirement
        date_et = spice.utc2et(str(date_jd) + " JD UTC")

        # Compute geocentric positions (x,y,z) in km for each date with light time correction
        r_geo, lt_ast = spice.spkpos(spkid, date_et, 'J2000', 'LT', '399')

        lon, lat, ele = location
        l_ra, l_dec = [], []

        # Convert from geocentric to topocentric coordinates
        r_topo = r_geo - geo_topo_vector(lon, lat, ele, float(date_jd))

        # Convert rectangular coordinates (x,y,z) to range, right ascension, and declination.
        d, rarad, decrad = spice.recrad(r_topo)

        # Transform RA and Decl. from radians to degrees.
        ra = np.degrees(rarad)
        dec = np.degrees(decrad)

        ccd.update({
            'date_et': date_et,
            'geocentric_positions': list(r_geo),
            'topocentric_positions': list(r_topo),
            'theoretical_coordinates': [ra, dec]
        })

        results.append(ccd)

    spice.kclear()

    return results


@python_app()
def theoretical_positions(asteroid, bsp_planetary, leap_second, observatory_location):

    from orbit_trace_library import compute_theoretical_positions
    import traceback

    try:
        # Recuperar o SPK Id do objeto a partir do seu BSP
        asteroid['spkid'] = findSPKID(
            asteroid['bsp_jpl']['path'])

        ccds = compute_theoretical_positions(
            asteroid['spkid'],
            asteroid['ccds'],
            asteroid['bsp_path'],
            bsp_planetary,
            leap_second,
            observatory_location
        )

        asteroid['ccds'] = ccds

    except Exception as e:
        trace = traceback.format_exc()

        msg = "Failed on retrive asteroids BSP from JPL. %s" % e

        asteroid['status'] = 'failure'
        asteroid['error'] = msg
        asteroid['traceback'] = trace

    finally:
        return asteroid

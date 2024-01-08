#!/usr/bin/python2.7
# -*- coding: utf-8 -*-

import csv
import math
import os
import subprocess

import spiceypy as spice

from library import HMS2deg


def findIDSPK(n, key):
    loc = 2  # order setting bsp files (1=DEXXX.bsp,2=Ast.bsp)
    m, header, flag = spice.dafec(loc, n)
    spk = ''
    for row in header:
        if row[:len(key)] == key:
            spk = row[len(key):].strip()
    return spk


# Function to compute the angle between two vector (return the angle in degrees)
def angle(v1, v2):
    rad = math.acos(dotproduct(v1, v2) / (norm(v1) * norm(v2)))
    return math.degrees(rad)


def dotproduct(v1, v2):
    return sum((a*b) for a, b in zip(v1, v2))


def norm(v):
    return math.sqrt(dotproduct(v, v))


def ra2HMS(rarad=''):
    radeg = math.degrees(rarad)
    raH = int(radeg/15.0)
    raM = int((radeg/15.0 - raH)*60)
    raS = 60*((radeg/15.0 - raH)*60 - raM)
    RA = '{:02d} {:02d} {:07.4f}'.format(raH, raM, raS)
    return RA


def dec2DMS(decrad=''):
    decdeg = math.degrees(decrad)
    ds = '+'
    if decdeg < 0:
        ds, decdeg = '-', abs(decdeg)
    deg = int(decdeg)
    decM = abs(int((decdeg - deg)*60))
    decS = 60*(abs((decdeg - deg)*60)-decM)
    DEC = '{}{:02d} {:02d} {:06.3f}'.format(ds, deg, decM, decS)
    return DEC


def generate_ephemeris(dates_file, bsp, dexxx, leap_sec, eph_filename, radec_filename):

    app_path = os.environ.get("APP_PATH").rstrip('/')
    data_dir = os.environ.get("DIR_DATA").rstrip('/')

    output_eph = os.path.join(data_dir, eph_filename)
    eph_link = os.path.join(app_path, eph_filename)

    output_radec = os.path.join(data_dir, radec_filename)
    radec_link = os.path.join(app_path, radec_filename)

    # Load the asteroid and planetary ephemeris and the leap second (in order)
    spice.furnsh(dexxx)
    spice.furnsh(leap_sec)
    spice.furnsh(bsp)

    # Values specific for extract all comments of header from bsp files (JPL, NIMA)
    source = {'NIMA': (45, 'ASTEROID_SPK_ID ='),
              'JPL': (74, 'Target SPK ID   :')}
    n, key = source['NIMA']
    idspk = findIDSPK(n, key)
    if idspk == '':
        n, key = source['JPL']
        idspk = findIDSPK(n, key)

    # Read the file with dates
    with open(dates_file, 'r') as inFile:
        dates = inFile.read().splitlines()

    n = len(dates)

    # Convert dates from utc to et format
    datesET = [spice.utc2et(utc) for utc in dates]

    # Compute geocentric positions (x,y,z) for each date with light time correction
    rAst, ltAst = spice.spkpos(idspk, datesET, 'J2000', 'LT', 'EARTH')
    rSun, ltSun = spice.spkpos('SUN', datesET, 'J2000', 'NONE', 'EARTH')

    elongation = [angle(rAst[i], rSun[i]) for i in range(n)]

    data = [spice.recrad(xyz) for xyz in rAst]
    distance, rarad, decrad = zip(*data)

    # ================= for graphics =================
    radecFile = open(output_radec, 'w')
    for row in data:
        radecFile.write(str(row[1]) + ';' + str(row[2]) + '\n')
    radecFile.close()

    # Altera permissão do arquivo para escrita do grupo
    os.chmod(output_radec, 0664)
    # Cria um link simbolico no diretório app
    os.symlink(output_radec, radec_link)

    # ================================================

    ra = [ra2HMS(alpha) for alpha in rarad]
    dec = [dec2DMS(delta) for delta in decrad]

    # Convert cartesian to angular coordinates and save it in a ascii file
    outFile = open(output_eph, 'w')
    outFile.write('\n\n     Data Cal. UTC' + ' '.ljust(51) +
                  'R.A.__(ICRF//J2000.0)__DEC')
    outFile.write(' '.ljust(43) + 'DIST (km)' + ' '.ljust(24) + 'S-O-A\n')
    for i in range(n):
        outFile.write(dates[i] + ' '.ljust(44) + ra[i] +
                      '  ' + dec[i] + ' '.ljust(35))
        outFile.write('{:.16E}'.format(distance[i]) + ' '.ljust(17))
        outFile.write('{:.4f}'.format(elongation[i]) + '\n')
    outFile.close()

    if os.path.exists(output_eph):

        # Altera permissão do arquivo para escrita do grupo
        os.chmod(output_eph, 0664)
        # Cria um link simbolico no diretório app
        os.symlink(output_eph, eph_link)

        return output_eph
    else:
        raise (Exception("Ephemeris file not generated. [%s]" % output_eph))


def generate_positions(eph_filename, positions_filename):

    app_path = os.environ.get("APP_PATH").rstrip('/')
    data_dir = os.environ.get("DIR_DATA").rstrip('/')

    output_pos = os.path.join(data_dir, positions_filename)
    pos_link = os.path.join(app_path, positions_filename)

    with open(output_pos, 'w') as fp:
        p = subprocess.Popen(
            ['gerapositions', eph_filename], stdin=subprocess.PIPE, stdout=fp)
        p.communicate()

    if os.path.exists(output_pos):

        # Altera permissão do arquivo para escrita do grupo
        os.chmod(output_pos, 0664)
        # Cria um link simbolico no diretório app
        os.symlink(output_pos, pos_link)

        return output_pos
    else:
        raise (Exception("Positions file not generated. [%s]" % output_pos))


def run_elimina(eph_filename, centers_filename):

    app_path = os.environ.get("APP_PATH").rstrip('/')
    data_dir = os.environ.get("DIR_DATA").rstrip('/')

    output = os.path.join(data_dir, centers_filename)
    out_link = os.path.join(app_path, centers_filename)

    strParameters = '\n'.join(map(str, [eph_filename]))
    with open(output, 'w') as outFile:
        # open the script .sh with the necessary configurations
        p = subprocess.Popen(
            'elimina', stdin=subprocess.PIPE, stdout=outFile, shell=True)

    # set the input parameters to the script
    p.communicate(strParameters)

    if os.path.exists(output):
        # Altera permissão do arquivo para escrita do grupo
        os.chmod(output, 0664)
        # Cria um link simbolico no diretório app
        os.symlink(output, out_link)

        return output
    else:
        raise (Exception("Centers file not generated. [%s]" % output))


def centers_positions_to_deg(centers_file, centers_deg_filename):

    app_path = os.environ.get("APP_PATH").rstrip('/')
    data_dir = os.environ.get("DIR_DATA").rstrip('/')

    output = os.path.join(data_dir, centers_deg_filename)
    out_link = os.path.join(app_path, centers_deg_filename)

    a_radec = list()
    with open(centers_file, 'r') as f:

        with open(output, 'w') as csvfile:
            fieldnames = ['ra', 'dec']
            writer = csv.DictWriter(
                csvfile, fieldnames=fieldnames, delimiter=";")
            writer.writeheader()

            for line in f:
                ra_hms, dec_hms = line.split('  ')
                radec = HMS2deg(ra_hms, dec_hms)
                a_radec.append(radec)

                writer.writerow({'ra': radec[0], 'dec': radec[1]})

    if os.path.exists(output):
        # Altera permissão do arquivo para escrita do grupo
        os.chmod(output, 0664)
        # Cria um link simbolico no diretório app
        os.symlink(output, out_link)

        return a_radec
    else:
        raise (Exception("Centers Deg file not generated. [%s]" % output))

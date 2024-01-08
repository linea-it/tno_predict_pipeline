import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

#Function to do a translation in RA from [pi --> 2pi | 0 --> pi] to [-pi --> 0 --> pi]
#input: list with elements in degrees (0-->360)
#output: list with elements in radians 
def translationRA(ra):
    newRA = [rai - 360 if rai > 180 else rai for rai in ra] 
    return np.deg2rad(newRA)


#Function to convert from -180 --> 180 to 0 --> 24 h or 0 --> 360 degrees
def deg2hms(degrees, ref = 'sexagesimal'):
    factor = 15
    sym = 'h'
    if ref == 'degrees':
        factor = 1
        sym = u'\N{DEGREE SIGN}'
    hours = [str((x + 360)/factor) + sym if x<0 else str(x/factor) + sym for x in degrees]
    return hours


def plotOrbit(asteroid, DESfootprint, planes, ephemeris, output):
    #limits for plot aitoff projection
    RAmin, RAmax = -180, 180
    DECmin, DECmax = -90, 90

    stepRA, stepDEC = 60, 30

    raAst, decAst = np.loadtxt(ephemeris, unpack=True)
    raAst_rad, decAst_rad = translationRA(raAst), np.deg2rad(decAst)

    #get DES footprint coordinates
    #https://github.com/kadrlica/skymap/blob/master/skymap/data/des-round17-poly.txt
    raF_deg, decF_deg = np.loadtxt(DESfootprint, unpack=True)
    raF_rad, decF_rad = np.deg2rad(raF_deg), np.deg2rad(decF_deg)

    #Get data to plot ecliptic and galactic planes
    galLon, galLat, ecLon, ecLat = np.loadtxt(planes, delimiter=',', skiprows=1, unpack=True)
    galLon_rad, galLat_rad = np.deg2rad(galLon), np.deg2rad(galLat)
    ecLon_rad, ecLat_rad = np.deg2rad(ecLon), np.deg2rad(ecLat)

    #setting the ticks labels, considering the minimum and maximum value and the step
    axisRAdeg  = range(RAmin, RAmax + 1, stepRA)
    axisDECdeg = range(DECmin,DECmax+ 1, stepDEC)

    #Transformation to Python projection for RA in units of hours or degrees
    ticksRA = deg2hms(axisRAdeg, 'degrees')
    ticksRA[0] = ticksRA[len(ticksRA) - 1] = ''

    plt.figure(figsize=(10, 8), dpi=90)
    plt.subplot(111, projection="aitoff")

    plt.plot(raAst_rad, decAst_rad, 'r', label = asteroid, zorder = 4)
    plt.plot(raF_rad, decF_rad, 'k', label = 'DES footprint', zorder = 6)
    plt.plot(galLon_rad, galLat_rad, 'm',  ms = 4, label = 'Galactic plane', zorder = 2)
    plt.plot(ecLon_rad, ecLat_rad, 'b',  ms = 4, label = 'Ecliptic plane', zorder = 1)

    #Redefine the ticks labels
    plt.xticks(np.deg2rad(axisRAdeg), ticksRA, weight='bold')
    plt.yticks(np.deg2rad(axisDECdeg), weight='bold')

    plt.grid(True)
    plt.legend(bbox_to_anchor=(1.05, 1.1))
    plt.savefig(output, bbox_inches='tight')



# if __name__ == "__main__":
#     #Input file names
#     objectName = "Eris"
#     filenameFootprint = "des-round17-poly.txt"
#     filenamePlanes = "eclipticGalacticData.csv"
#     astFilename = "/data/positions.txt"
#     outputFilename = "/data/asteroidOrbit.png"

#     plotOrbit(objectName, filenameFootprint, filenamePlanes, astFilename, outputFilename)




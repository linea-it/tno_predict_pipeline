from datetime import datetime
import numpy as np
import subprocess
import os


#Function to execute scripts, the parameters must be an numpy array 
#with specific and ordered values
#for example the script sc_wget needs [number, name, par1, par2]
#where par1 is the parameter for creation of observations file
#and par2 is the parameter for creation of orbital elements file
#errors for this example: [name, number, par1, par2]
#[number, name, par2, par1], [number, par1, name, par2], etc.
def executeScript(script, parameters):

    strParameters = '\n'.join(map(str, parameters))

    #open the script .sh with the necessary configurations
    p = subprocess.Popen(script, stdin=subprocess.PIPE, shell=True)

    #set the input parameters to the script
    p.communicate(strParameters)


def NIMAmanager(inputParametersFile):
    parameters, comment = np.loadtxt(inputParametersFile, dtype='str', delimiter='|', converters={0: lambda s: s.strip()}, unpack=True)

    pathNIMAuser = parameters[0]
    number = parameters[5]
    name = parameters[6]

    #Check if necessary files exist (if not then it is finalized here)
    if os.path.exists(parameters[3] + '/' + name + ".rwo") and os.path.exists(parameters[4] + '/' + name + ".eq0"):
        pass
    elif os.path.exists(parameters[3] + '/' + name + ".rwm") and os.path.exists(parameters[4] + '/' + name + ".eqm"):
        pass
    else:
        print "There are no files of the ", name
        print exit(0)

    asteroidFolder = pathNIMAuser + "/results/" + number

    myPath = os.getcwd()

    #changing from local path to especific path 
    os.chdir(pathNIMAuser)
    

    #create the folder "number" if it does not exist    
    if os.path.exists(asteroidFolder):
        subprocess.call(["rm", "-r", asteroidFolder])
    os.mkdir(asteroidFolder)

    #copy the bsp file (JPL) and astrometry file (PRAIA) to NIMA
    subprocess.call(["cp", parameters[1] + "/" + name + ".txt", asteroidFolder])
    subprocess.call(["cp", parameters[2] + "/" + name + ".bsp", pathNIMAuser + "/jplbsp"])


    #============================= EXECUTE ALL SCRIPTS NIMA =============================
    
    #print "========================== sc_AstDySMPC2NIMA =========================="

    executeScript("./sc_AstDySMPC2NIMA.sh", np.concatenate([parameters[5:7], parameters[3:5]]) )


    #print "============================== sc_esoopd =============================="

    executeScript("./sc_esoopd.sh", parameters[5:7])
    
    #print "=============================== sc_cat ================================"

    executeScript("./sc_cat.sh", np.insert(parameters[7:8],0,number))

    #print "============================== sc_merge ==============================="

    executeScript("./sc_merge.sh", np.insert(parameters[8:10],0,number))

    #print "=============================== sc_fit ================================"

    executeScript("./sc_fit.sh", np.insert(parameters[10:17],0,number))

    #print "============================= sc_importbsp ============================"

    executeScript("./sc_importbsp.sh", np.append(parameters[5:7],"jplbsp/" + name + ".bsp"))

    #print "============================ sc_diffjplomc ============================"
    
    executeScript("./sc_diffjplomcPython.sh", np.insert(parameters[17:31],0,number))

    
    #print "============================== sc_makebsp ============================="
    
    executeScript("./sc_makebsp.sh", np.insert(parameters[31:38],0,number))

    #print "=============================== sc_ephem =============================="

    executeScript("./sc_ephem.sh", np.insert(parameters[38:],0,number))

    # TODO: COPIAR ARQUIVO DE DUPLICIDADE

    #print "============================ move results =============================="

    resultFolder = os.path.join(os.environ.get("DIR_RESULTS"), name.replace(' ', '_'))

    subprocess.call(["mv", asteroidFolder, resultFolder])

    subprocess.call(["chmod", "-R", "775", resultFolder])

if __name__ == "__main__":

    # parametersFile = raw_input("Write the file name with all NIMA parameters: ")

    parametersFile = os.path.join(os.environ.get("DIR_INPUTS"), "input.txt")

    start_time = datetime.now()

    print parametersFile

    NIMAmanager(parametersFile)
    
    end_time = datetime.now()

    print "Duration: ", end_time - start_time


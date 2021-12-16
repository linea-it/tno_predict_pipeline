from external_inputs.download import Download
import pathlib

def getFileURL(name, number, key, ext):
    base_url = "https://newton.spacedys.com/~astdys2/%s/" % key

    if number is not None and number != '' and number != "-" and number != 'None':
        link2 = "numbered/" + str(int((int(number) / 1000))) + "/" + number + ext
    else:
        temp_name = name.replace(' ', '')
        try:
            year = int(temp_name[:4])
        except ValueError:
            msg = "some error happened with the object: %s" % name
            raise Exception(msg)

        if year >= 1998:
            link2 = "unumbered/" + temp_name[:5] + "/" + temp_name + ext
        elif year > 1980 and year < 1998:
            link2 = "unumbered/" + temp_name[:4] + "/" + temp_name + ext
        elif year >= 1920 and year <= 1980:
            link2 = "unumbered/" + temp_name[:3] + "0" + "/" + temp_name + ext

    url = base_url + link2

    return url

def getOrbitalParametersURL(name, number):
    return getFileURL(name, number, "epoch", ".eq0")

def getObservationsURL(name, number):
    return getFileURL(name, number, "mpcobs", ".rwo")


def get_astdys_orbital_elements(name, number, output_path):

    url = getOrbitalParametersURL(name, number)
    filename = '{}.eq0'.format(name.replace(' ', ''))

    try:
        file_path, download_stats = Download().download_file_from_url(
            url, 
            output_path=output_path, 
            filename=filename,
            overwrite=True, 
            ignore_errors=False, 
            timeout=5
        )
        
        return pathlib.Path(file_path)

    except Exception as e:
        return None

def get_astdys_observations(name, number, output_path):
    url = getObservationsURL(name, number)
    filename = '{}.rwo'.format(name.replace(' ', ''))
    try:
        file_path, download_stats = Download().download_file_from_url(
            url, 
            output_path=output_path, 
            filename=filename,
            overwrite=True, 
            ignore_errors=False, 
            timeout=5
        )
        
        return pathlib.Path(file_path)

    except Exception as e:
        return None

 




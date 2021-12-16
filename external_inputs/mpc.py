from astroquery.mpc import MPC
from datetime import datetime as dt
import numpy as np
import astropy
import pathlib
import json
astropy.utils.data.clear_download_cache()


def get_identifier(name, number=None):
    """Function to define the parameter (number or name) to be used

    Args:
        name (str): Name of a object
        number (str): Number of a object

    Returns:
        str: Number if numbered or name if nonumbered (number as priority)
    """
    obj_id = number
    if number is None or number == '-':
        obj_id = name

    return obj_id


def fill_empty_item(dict_obj):
    """Function to fill item (number and/or name) with a string "no" (NIMA requerement)

    Args:
        dict_obj ([type]): [description]
    """
    value = 'no'
    keys = ['number', 'name']
    for key in keys:
        if not dict_obj[key]:
            dict_obj[key] = value


def get_mpc_observations(name, output_path, number=None):

    # output file name
    filename = '{}.rwm'.format(name.replace(' ', ''))

    filepath = pathlib.Path(output_path, filename)

    identifier = get_identifier(name, number)

    # getting all registered observations
    obs = MPC.get_observations(identifier, get_mpcformat=True)
    obs_data = obs['obs'].data
    np.savetxt(filepath, obs_data, fmt='%s')

    return filepath


def get_mpc_orbital_elements(name, output_path, number=None):

    filename = 'orbital_elements.json'
    filepath = pathlib.Path(output_path, filename)

    identifier = get_identifier(name, number)

    if identifier == number:
        orbital = MPC.query_object(
            'asteroid', number=identifier)
    else:
        orbital = MPC.query_object(
            'asteroid', designation=identifier)

    try:
        parameters = orbital[0]

        parameters['object_ID'] = identifier

        with open(filepath, 'w') as json_file:
            json.dump(parameters, json_file)

        return filepath
    except Exception as e:
        # Não retornou resultado do MPC
        return None


def generate_resumed_orbital_elements(name, input_file, output_path):

    # Le o arquivo orbital_elements.json
    with open(input_file, 'r') as json_file:
        data = json.load(json_file)

    # output file name
    filename = '{}.eqm'.format(name.replace(' ', ''))
    filepath = pathlib.Path(output_path, filename)

    # keywords for the query in MPC
    keys = ['object_ID', 'number', 'name', 'epoch', 'epoch_jd', 'argument_of_perihelion',
            'mean_anomaly', 'ascending_node', 'inclination', 'eccentricity',
            'semimajor_axis', 'absolute_magnitude', 'phase_slope']

    # (NIMA requerement)
    data['number'] = 'no'
    data['name'] = 'no'

    with open(filepath, 'w') as output_file:
        for key in keys:
            value = str(data[key])
            output_file.write(value + '\n')

    return filepath

# def getMPCdata(number, name, outputPath='./'):
#     # extensions for output files
#     obs_ext = '.rwm'
#     orb_ext = '.eqm'

#     # output file name (observation and orbital elements)
#     obsFilename = name.replace(' ', '') + obs_ext
#     orbFilename = name.replace(' ', '') + orb_ext

#     # keywords for the query in MPC
#     keys = ['designation', 'number', 'name', 'epoch', 'epoch_jd', 'argument_of_perihelion',
#             'mean_anomaly', 'ascending_node', 'inclination', 'eccentricity',
#             'semimajor_axis', 'absolute_magnitude', 'phase_slope']

#     keys_str = ', '.join(keys)

#     identifier = get_identifier(name, number)

#     # getting all registered observations
#     obs = MPC.get_observations(identifier, get_mpcformat=True)
#     obs_data = obs['obs'].data
#     np.savetxt(outputPath + obsFilename, obs_data, fmt='%s')

#     # getting orbital elements
#     if identifier == number:
#         orbital = MPC.query_object(
#             'asteroid', number=identifier, return_fields=keys_str)
#     else:
#         orbital = MPC.query_object(
#             'asteroid', designation=identifier, return_fields=keys_str)

#     parameters = orbital[0]

#     newKey = 'object_ID'

#     parameters[newKey] = identifier
#     keys[0] = newKey

#     fill_empty_item(parameters)

#     outFile = open(outputPath + orbFilename, 'w')

#     for key in keys:
#         value = str(parameters[key])
#         #print('{:25}'.format(key), ':', value)
#         outFile.write(value + '\n')

#     outFile.close()


# if __name__ == '__main__':
#     get_mpc_orbital_elements('137295', '1999 RB216')
#     get_mpc_observations('137295', '1999 RB216')

#     get_mpc_orbital_elements('137295', 'Chariklo')
#     get_mpc_observations('137295', 'Chariklo')

#     get_mpc_orbital_elements('-', '1999 RC216')
#     get_mpc_observations('-', '1999 RC216')

#     getMPCdata('136199', 'Eris')

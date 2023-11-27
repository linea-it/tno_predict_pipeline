# def get_bsp_from_jpl(identifier, initial_date, final_date, directory, filename):
#     """Download bsp files from JPL database

#         Bsp files, which have information to generate
#         the ephemeris of the objects, will be downloaded
#         The files will be named as (without spaces): [identifier].bsp

#         Important:
#             it is able to download bsp files of neither planets nor satellites

#     Args:
#         identifier (str): Identifier of the object.
#             It can be the name, number or SPK ID.
#             It can also be a list of objects.
#             Examples:
#                 '2137295'
#                 '1999 RB216'
#                 '137295'
#                 'Chariklo'
#         initial_date (str): Date the bsp file is to begin, within span [1900-2100].
#             Examples:
#                 '2003-02-01'
#                 '2003-3-5'
#         final_date (str): Date the bsp file is to end, within span [1900-2100].
#             Must be more than 32 days later than [initial_date].
#             Examples:
#                 '2006-01-12'
#                 '2006-1-12'

#         directory (str): Directory path to save the bsp files.

#     Returns:
#         Path: file path for .bsp file.
#     """

#     date1 = dt.strptime(initial_date, '%Y-%m-%d')
#     date2 = dt.strptime(final_date, '%Y-%m-%d')
#     diff = date2 - date1

#     if diff.days <= 32:
#         raise ValueError(
#             'The [final_date] must be more than 32 days later than [initial_date]')
     
#     path = pathlib.Path(directory)
#     if not path.exists():
#         raise ValueError('The directory {} does not exist!'.format(path))
        
#     # Define API URL and SPK filename:
#     url = 'https://ssd.jpl.nasa.gov/api/horizons.api'
#     spk_filename = path.joinpath(filename)

#     # Define the time span:
#     start_time = date1.strftime('%Y-%b-%d')
#     stop_time = date2.strftime('%Y-%b-%d')

#     spkid = identifier

#     # Build the appropriate URL for this API request:
#     # IMPORTANT: You must encode the "=" as "%3D" and the ";" as "%3B" in the
#     #            Horizons COMMAND parameter specification.
#     url += "?
#     format=json
#     &EPHEM_TYPE=SPK
#     &OBJ_DATA=YES"
#     url += "&COMMAND='{}
#     '&START_TIME='{}
#     '&STOP_TIME='{}'".
#     format(spkid, start_time, stop_time)

#     # Submit the API request and decode the JSON-response:
#     response = requests.get(url)
#     try:
#       data = json.loads(response.text)
#     except ValueError:
#       print("Unable to decode JSON results")

#     # If the request was valid...
#     if (response.status_code == 200):
#       #
#       # If the SPK file was generated, decode it and write it to the output file:
#       if "spk" in data:
#         #
#         try:
#           f = open(spk_filename, "wb")
#         except OSError as err:
#           print("Unable to open SPK file '{0}': {1}".format(spk_filename, err))
#         #
#         # Decode and write the binary SPK file content:
#         f.write(base64.b64decode(data["spk"]))
#         f.close()
#         print("wrote SPK content to {0}".format(spk_filename))
#         sys.exit()
#       #
#       # Otherwise, the SPK file was not generated so output an error:
#       print("ERROR: SPK file not generated")
#       if "result" in data:
#         print(data["result"])
#       else:
#         print(response.text)
#       sys.exit(1)

#     # If the request was invalid, extract error content and display it:
#     if (response.status_code == 400):
#       data = json.loads(response.text)
#       if "message" in data:
#         print("MESSAGE: {}".format(data["message"]))
#       else:
#         print(json.dumps(data, indent=2))

#     # Otherwise, some other error occurred:
#     print("response code: {0}".format(response.status_code))
#     sys.exit(2)
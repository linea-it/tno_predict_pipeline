# Exemplo URL para Download dos ccds.
# https://desar2.cosmology.illinois.edu/DESFiles/desarchive/OPS/finalcut/Y5A1/r3516/20171021/D00689749/p01/cat/D00689749_z_c24_r3516p01_red-fullcat.fits
from library import retrieve_asteroids
import os
from asteroid import Asteroid
import pathlib
import urllib.parse
import csv
from external_inputs.download import Download
import humanize

original_path = os.getcwd()
os.environ["EXECUTION_PATH"] = original_path

LEAP_SECOND = "/lustre/t1/tmp/tno/leap_seconds/naif0012.tls"
# DES_CATALOG_PATH = "/archive/cl/ton"
DES_CATALOG_PATH = "/lustre/t1/tmp/tno/temp_catalogs"


# DES_CATALOG_PATH = "/archive/des/public"
BASE_DES_URL = "https://desar2.cosmology.illinois.edu/DESFiles/desarchive/OPS/"
DOWNLOAD_MISSING_FILES = True

#asteroids = retrieve_asteroids("name", "Eris")
#centaurs = retrieve_asteroids("base_dynclass", "Centaur")
kbos = retrieve_asteroids("base_dynclass", "KBO")
# asteroids = centaurs + kbos
asteroids = kbos[0:300]
print("Asteroids: %s" % len(asteroids))

ccds_to_download = list()
ccds_total = 0
ccds_missing_total = 0

for asteroid in asteroids:
    a = Asteroid(
        id=asteroid["id"],
        name=asteroid["name"],
        number=asteroid["number"],
        base_dynclass=asteroid["base_dynclass"],
        dynclass=asteroid["dynclass"],
    )

    a.set_log("orbit_trace")

    ccds = a.retrieve_ccds(LEAP_SECOND)
    #print(ccds)
    missing = 0
    downloaded = 0
    downloaded_size = 0
    for ccd in ccds:
        ccd_path = pathlib.Path(ccd["path"], ccd["filename"])
        local_path = pathlib.Path(DES_CATALOG_PATH, ccd_path)

        if not local_path.exists():
            to_download = ccd
            to_download["local_path"] = local_path
            to_download["download_url"] = urllib.parse.urljoin(
                BASE_DES_URL, str(ccd_path)
            )

            ccds_to_download.append(to_download)
            missing += 1
            if DOWNLOAD_MISSING_FILES:

                output_path = pathlib.Path(DES_CATALOG_PATH, to_download["path"])
                output_path.mkdir(exist_ok=True, parents=True)

                f, dw_status = Download().download_file_from_url(
                    url=to_download["download_url"],
                    output_path=output_path,
                    filename=to_download["filename"],
                    auth=("brportal", "Des123456"),
                )

                to_download["downloaded"] = True
                to_download["download_time"] = dw_status["download_time"]
                to_download["file_size"] = dw_status["file_size"]

                downloaded += 1
                downloaded_size += dw_status["file_size"]

    print("Asteroid: %s CCDs: %s Missing: %s" % (a.name, len(ccds), missing))
    ccds_total += len(ccds)
    ccds_missing_total += missing

print("Total CCDs: %s" % ccds_total)
print("Total To Download: %s" % ccds_missing_total)
print(
    "Total Downloaded: %s  Size: %s"
    % (downloaded, humanize.naturalsize(downloaded_size))
)

# Escreve um csv com todos os ccds que estão faltando no diretório.
ccds_csv_filename = pathlib.Path("ccds_downloaded.csv")
with open(ccds_csv_filename, "w") as csvfile:
    fieldnames = ["id", "path", "filename", "download_url"]
    writer = csv.DictWriter(
        csvfile, fieldnames=fieldnames, delimiter=";", extrasaction="ignore"
    )

    writer.writeheader()
    writer.writerows(ccds_to_download)

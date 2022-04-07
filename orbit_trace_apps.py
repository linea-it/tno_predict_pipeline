# -*- coding: utf-8 -*-
import os
from parsl import python_app


@python_app()
def theoretical_positions(asteroid, bsp_planetary, leap_second, observatory_location):

    from library import compute_theoretical_positions, get_logger
    import traceback
    from datetime import datetime, timezone

    tp0 = datetime.now(tz=timezone.utc)

    log = get_logger(asteroid["path"], "theo_pos.log")

    try:
        ccds = compute_theoretical_positions(
            asteroid["spkid"],
            asteroid["ccds"],
            str(asteroid["bsp_path"]),
            bsp_planetary,
            leap_second,
            observatory_location,
            log,
        )

        asteroid["ccds"] = ccds

    except Exception as e:
        trace = traceback.format_exc()

        msg = "Failed Compute Theoretical Positions. %s" % e

        asteroid["status"] = "failure"
        asteroid["error"] = msg
        asteroid["traceback"] = trace

    finally:

        del log
        tp1 = datetime.now(tz=timezone.utc)

        # Orbit Trace Theoretical Positions Time Profile
        asteroid.update(
            {
                "ot_theo_pos": dict(
                    {"tp_start": tp0.isoformat(), "tp_finish": tp1.isoformat()}
                )
            }
        )

        return asteroid


@python_app()
def observed_positions(idx, name, asteroid_id, ccd, asteroid_path, radius=2):

    import os
    import numpy as np
    from astropy.io import fits
    from library import get_logger

    import numpy as np
    from astropy import units as u
    from astropy.coordinates import SkyCoord
    import traceback

    obs_coordinates = None

    # Cria um log no diretório do Asteroid, 1 log para cada CCD.
    log = get_logger(asteroid_path, "des_obs_%s.log" % (idx))

    log.info("Calculating observed positions")
    log.debug("Asteroid: %s" % name)
    log.debug("IDX: %s" % idx)
    log.debug("CCD: %s" % ccd["path"])

    try:
        # Coordenadas teroricas
        ra_jpl, dec_jpl = ccd["theoretical_coordinates"]

        file_path = os.path.join(ccd["path"], ccd["filename"])

        # Le o catalogo Fits
        hdul = fits.open(file_path)
        rows = hdul[2].data

        a_ra_des = rows["ALPHA_J2000"]
        a_dec_des = rows["DELTA_J2000"]
        a_magpsf = rows["MAG_PSF"]
        a_magerrpsf = rows["MAGERR_PSF"]

        catJPL = SkyCoord(ra=[ra_jpl] * u.deg, dec=[dec_jpl] * u.deg, frame="icrs")
        catD00 = SkyCoord(ra=a_ra_des * u.deg, dec=a_dec_des * u.deg, frame="icrs")
        id1, d2d1, d3d = catD00.match_to_catalog_sky(catJPL)

        # search the index and value of element with minimal distance
        IDX_D00 = np.argmin(d2d1.arcsec)
        dmin = d2d1[IDX_D00].arcsec

        ra, dec, match_idx = None, None, None
        if dmin < radius:
            ra = a_ra_des[IDX_D00]
            dec = a_dec_des[IDX_D00]
            match_idx = IDX_D00

        if ra is not None and dec is not None:
            # Calcular "observed minus calculated"
            observed_omc = [
                float((ra - ra_jpl) * 3600 * np.cos(np.radians(dec_jpl))),
                float((dec - dec_jpl) * 3600),
            ]
            ccd.update(
                {
                    "observed_coordinates": [float(ra), float(dec)],
                    "observed_omc": observed_omc,
                    "magpsf": float(a_magpsf[match_idx]),
                    "magerrpsf": float(a_magerrpsf[match_idx]),
                }
            )

            # Cria um objeto com os dados da posição observada,
            # Esta etapa está aqui para aproveitar o paralelismo e adiantar a consolidação.
            obs_coordinates = dict(
                {
                    "name": name,
                    "asteroid_id": asteroid_id,
                    "ccd_id": ccd["id"],
                    "date_obs": ccd["date_obs"],
                    "date_jd": ccd["date_jd"],
                    "ra": ccd["observed_coordinates"][0],
                    "dec": ccd["observed_coordinates"][1],
                    "offset_ra": ccd["observed_omc"][0],
                    "offset_dec": ccd["observed_omc"][1],
                    "mag_psf": ccd["magpsf"],
                    "mag_psf_err": ccd["magerrpsf"],
                }
            )

            log.info("Observed Coordinates: %s" % ccd["observed_coordinates"])

        else:
            ccd.update(
                {"observed_coordinates": None, "info": "Could not find a position."}
            )

            log.info("Could not find a position.")

    except Exception as e:
        ccd.update(
            {
                "observed_coordinates": None,
                "error": "Failed in match position stage with exception: %s" % str(e),
            }
        )

        trace = traceback.format_exc()
        log.error(trace)

        log.error("Failed in match position stage with exception: %s" % str(e))

    finally:
        log.info("Calculating observed positions Finish")
        return (name, ccd, obs_coordinates)

# occviz.py
# Author: Rodrigo Boufleur July 2023
# Last update: October 2023

# The _xy2latlon function is based on the function xy2latlon from the SORA v0.3.1 lib

# import astropy.config.paths
# print("--------------------------------")
# print(astropy.config.paths.get_cache_dir())
# print("--------------------------------")
# from astropy.config import set_temp_cache
# set_temp_cache('/app/shared/cache')

import os
import numpy as np
import astropy.units as u
import astropy.constants as const
from astropy.time import Time
from astropy.coordinates import SkyCoord, Angle, GCRS, ITRS, get_sun, EarthLocation, SkyOffsetFrame
from scipy.interpolate import CubicSpline
from typing import Optional, Union
from datetime import datetime, timezone
import json


def _calculate_r2(x, y):
    """
    Calculate r2 based on the given x and y values.

    Parameters
    ----------
    x : numpy.ndarray
        Projected position in x.
    y : numpy.ndarray
        Projected position in y.

    Returns
    -------
    numpy.ndarray
        Calculated r2 values.
    """
    r2 = const.R_earth.to_value(u.m)**2 - (x**2 + y**2)
    return r2


def _get_valid_indices(r2):
    """
    Get valid indices where r2 is greater than or equal to 0.0.

    Parameters
    ----------
    r2 : numpy.ndarray
        Array of r2 values.

    Returns
    -------
    numpy.ndarray
        Valid indices where r2 >= 0.0.
    """
    return np.where(r2 >= 0.0)[0]


def _transform_coordinates(r, x, y, time, loncen, latcen, true_idx, r2):
    """
    Transform coordinates to get longitude and latitude.

    Parameters
    ----------
    r : numpy.ndarray
        Array of r values.
    x : numpy.ndarray
        Projected position in x.
    y : numpy.ndarray
        Projected position in y.
    time : astropy.time.Time
        Time of referred projection.
    loncen : int, float
        Center longitude of projection, in degrees.
    latcen : int, float
        Center latitude of projection, in degrees.
    true_idx : numpy.ndarray
        Valid indices where r2 >= 0.0.

    Returns
    -------
    tuple
        Longitude and latitude values.
    """
    if (not time.isscalar) and (len(time) == len(r2)):
        time, loncen, latcen = time[true_idx], loncen[true_idx], latcen[true_idx]
        site_cen = EarthLocation(loncen*u.deg, latcen*u.deg)
        itrs_cen = site_cen.get_itrs(obstime=time)
        gcrs_cen = itrs_cen.transform_to(GCRS(obstime=time))
        center_frame = SkyOffsetFrame(origin=gcrs_cen)

        for n in range(5):
            new_pos = SkyCoord(
                r*u.m, x*u.m, y*u.m, representation_type='cartesian', frame=center_frame)
            n_coord = new_pos.transform_to(GCRS(obstime=time))
            n_itrs = n_coord.transform_to(ITRS(obstime=time))
            n_site = n_itrs.earth_location
            n_site = EarthLocation(n_site.lon, n_site.lat, 0)
            itrs_site = n_site.get_itrs(obstime=time)
            gcrs_site = itrs_site.transform_to(GCRS(obstime=time))
            target1 = gcrs_site.transform_to(center_frame)
            r = target1.cartesian.x.to(u.m).value

        return n_site.lon.deg, n_site.lat.deg
    else:
        return None, None


def _xy2latlon(x, y, loncen, latcen, time):
    """
    Calculates the longitude and latitude given projected positions x and y.

    Parameters
    ----------
    x : int, float
        Projected position in x, in the GCRS, in meters.
    y : int, float
        Projected position in y, in the GCRS, in meters.
    loncen : int, float
        Center longitude of projection, in degrees.
    latcen : int, float
        Center latitude of projection, in degrees.
    time : astropy.time.Time
        Time of referred projection.

    Returns
    -------
    tuple
        Longitude and latitude values.
    """
    x = np.array(x)
    y = np.array(y)
    r2 = _calculate_r2(x, y)

    true_idx = _get_valid_indices(r2)
    r, x, y = np.sqrt(r2[true_idx]), x[true_idx], y[true_idx]
    lon, lat = np.full(len(r2), 1e+31), np.full(len(r2), 1e+31)

    if len(r) > 0:
        lon[true_idx], lat[true_idx] = _transform_coordinates(
            r, x, y, time, loncen, latcen, true_idx, r2)
    return lon, lat


def _calculate_arc_coordinates(delta, ca, pa, dtimes, vel, pa_plus, radius):
    """
    Calculate the x and y coordinates of an arc based on the given parameters.

    Parameters
    ----------
    delta : Quantity
        The angular separation between two points on the sky.
    ca : Quantity
        The chord length between the two points.
    pa : Quantity
        The position angle of the chord.
    dtimes : array-like
        The time intervals.
    vel : Quantity
        The velocity of the object.
    pa_plus : Quantity
        The position angle plus 90 degrees.
    radius : Quantity
        The radius of the object.

    Returns
    -------
    tuple
        The x and y coordinates of the arc in meters.
    """
    arc = (delta.to(u.km) * ca.to(u.rad)).value * u.km
    arc_x = arc * np.sin(pa) + (dtimes * vel) * np.cos(pa_plus)
    arc_y = arc * np.cos(pa) - (dtimes * vel) * np.sin(pa_plus)
    arc_x = arc_x + (radius * u.km) * np.sin(pa_plus)
    arc_y = arc_y + (radius * u.km) * np.cos(pa_plus)

    return arc_x.to(u.m).value, arc_y.to(u.m).value


def _handle_longitude_discontinuities(lon):
    """
    Handle discontinuities in longitude values.

    Parameters
    ----------
    lon : numpy.ndarray
        Longitude values.

    Returns
    -------
    numpy.ndarray
        Adjusted longitude values.
    """

    # Calculate the differences between consecutive longitude values
    differences = np.diff(lon)
    threshold = 300                    # Set a threshold to detect discontinuities
    # Find indices where differences exceed the threshold
    factor_index = np.where(abs(differences) > threshold)

    # Check if discontinuities are detected
    if np.size(factor_index) > 0:
        # Initialize a factor to adjust longitude values
        factor = 1
        if differences[factor_index][0] > 0:
            index = _get_increasing_discontinuity_indices(
                factor_index, len(lon))
        else:
            index = _get_decreasing_discontinuity_indices(
                factor_index, len(lon))
    else:
        # No discontinuities detected, no adjustment needed
        factor = 0
        index = (0)

    lon[index] += factor * 360

    return lon


def _get_increasing_discontinuity_indices(factor_index, length):
    """
    Get indices for increasing discontinuities.

    Parameters
    ----------
    factor_index : numpy.ndarray
        Indices of detected discontinuities.
    length : int
        Length of the longitude array.

    Returns
    -------
    numpy.ndarray
        Indices to adjust for increasing discontinuities.
    """
    if np.size(factor_index) > 1:
        indexa = np.arange(0, factor_index[0][0] + 1, 1)
        indexb = np.arange(factor_index[0][1] + 1, length, 1)
        return np.concatenate([indexa, indexb])
    else:
        return np.arange(0, factor_index[0] + 1, 1)


def _get_decreasing_discontinuity_indices(factor_index, length):
    """
    Get indices for decreasing discontinuities.

    Parameters
    ----------
    factor_index : numpy.ndarray
        Indices of detected discontinuities.
    length : int
        Length of the longitude array.

    Returns
    -------
    numpy.ndarray
        Indices to adjust for decreasing discontinuities.
    """
    if np.size(factor_index) > 1:
        indexa = np.arange(0, factor_index[0][0] + 1, 1)
        indexb = np.arange(factor_index[0][1] + 1, length, 1)
        return np.concatenate([indexa, indexb])
    else:
        return np.arange(factor_index[0] + 1, length, 1)


def _interpolate_coordinates(lon, lat, times):
    """
    Interpolate longitude and latitude values.

    Parameters
    ----------
    lon : numpy.ndarray
        Longitude values.
    lat : numpy.ndarray
        Latitude values.
    times : numpy.ndarray
        Time values.

    Returns
    -------
    tuple
        Interpolated longitude and latitude values.
    """
    cs_lat = CubicSpline(times, lat)
    cs_lon = CubicSpline(times, lon)
    t_interp = np.linspace(times.min(), times.max(), 1275)
    lat_interp = cs_lat(t_interp)
    lon_interp = cs_lon(t_interp)
    lon_interp[lon_interp > 180] -= 360
    lon_interp[lon_interp < -180] += 360

    return lon_interp, lat_interp


def _path_latlon(instants, dtimes, centers, delta, ca, vel, pa, pa_plus, radius=0, interpolate=True):
    '''
    Private function that provides the latitudes and longitudes for the path.

    Parameters:
    - instants (list): A list of time instances for the path.
    - dtimes (list): A list of time intervals.
    - centers (SkyCoord): A SkyCoord object representing the center coordinates of the projection.
    - delta (Quantity): The angular separation between two points on the sky.
    - ca (Quantity): The chord length between the two points.
    - vel (Quantity): The velocity of the object.
    - pa (Quantity): The position angle of the chord.
    - pa_plus (Quantity): The position angle plus 90 degrees.
    - radius (Quantity, optional): The radius of the object (default is 0).
    - interpolate (bool, optional): A boolean flag indicating whether to interpolate the coordinates (default is True).

    Returns:
    - lon (array): An array of longitude values for the path.
    - lat (array): An array of latitude values for the path.

    Summary:
    The _path_latlon function calculates the latitudes and longitudes for a given path based on the input parameters. It can be used to generate the path of an occultation event.

    Example Usage:
    instants = [time1, time2, time3]
    dtimes = [dt1, dt2, dt3]
    centers = SkyCoord(lon=loncen, lat=latcen)
    delta = 0.5 * u.deg
    ca = 100 * u.km
    vel = 10 * u.km/u.s
    pa = 45 * u.deg
    pa_plus = pa + 90 * u.deg
    radius = 0 * u.km
    interpolate = True

    lon, lat = _path_latlon(instants, dtimes, centers, delta, ca, vel, pa, pa_plus, radius, interpolate)

    In this example, the function is called with the input parameters to calculate the latitudes and longitudes for the given path. The resulting lon and lat arrays will contain the longitude and latitude values respectively.
    '''

    arc_x, arc_y = _calculate_arc_coordinates(
        delta, ca, pa, dtimes, vel, pa_plus, radius)

    lon, lat = _xy2latlon(arc_x, arc_y, centers.lon.value,
                          centers.lat.value, instants)

    valid_coordinates = (lon < 1e+31)
    lon, lat, times = lon[valid_coordinates], lat[valid_coordinates], dtimes[valid_coordinates] - dtimes[0]
    if interpolate and (len(lon) > 2) and (len(lat) > 2):
        lonbkp = lon.copy()
        try:
            lon = _handle_longitude_discontinuities(lon)
            return _interpolate_coordinates(lon, lat, times)
        except:
            return [lonbkp, lat]
    else:
        return [lon, lat]


def _setup_initial_variables(date_time, star_coordinates, delta_distance, velocity, position_angle, closest_approach, offset):
    """
    Set up initial variables for the occultation path calculation.

    Parameters
    ----------
    date_time : str
        The date and time of the observation in the format 'YYYY-MM-DDTHH:MM:SS'.
    star_coordinates : tuple
        The coordinates of the star in the format (RA, Dec) where RA is in hours and Dec is in degrees.
    delta_distance : float
        The distance of the object from the observer in astronomical units (AU).
    velocity : float
        The velocity of the object relative to the observer in kilometers per second (km/s).
    position_angle : float
        The position angle of the object relative to the observer in degrees.
    closest_approach : float
        The closest approach of the object to the observer in arcseconds.
    offset : list
        The offset of the observer from the center of the object in milliarcseconds (mas).

    Returns
    -------
    tuple
        Initial variables for the occultation path calculation.
    """
    instant = Time(date_time)
    star = SkyCoord(star_coordinates, frame='icrs',
                    unit=(u.hourangle, u.degree))
    delta = delta_distance * u.AU
    vel = velocity * (u.km / u.s)
    pa = Angle(position_angle * u.deg)
    pa.wrap_at('180d', inplace=True)
    off_ra, off_dec = (offset[0] * u.mas, offset[1] * u.mas)
    delta_ca = off_ra * np.sin(pa) + off_dec * np.cos(pa)
    delta_instant = (-(off_ra * np.cos(pa) - off_dec * np.sin(pa)
                       ).to(u.rad) * delta.to(u.km) / abs(vel)).value * u.s
    ca = closest_approach * u.arcsec + delta_ca
    instant = instant + delta_instant

    return instant, star, delta, vel, pa, ca


def _calculate_position_angle(pa):
    """
    Calculate the position angle.

    Parameters
    ----------
    pa : Angle
        Initial position angle.

    Returns
    -------
    Angle
        Adjusted position angle.
    """
    if pa > 90 * u.deg:
        return pa - Angle(180 * u.deg)
    elif pa < Angle(-90 * u.deg):
        return pa + Angle(180 * u.deg)
    else:
        return Angle(pa)


def _generate_instants_array(vel):
    """
    Generate the array of instants.

    Parameters
    ----------
    instant : Time
        Instant of the closest approach.
    vel : Quantity
        Object velocity.

    Returns
    -------
    numpy.ndarray
        Array of instants.
    """
    dtimes = np.linspace(0, int(6371 / abs(vel.value)), 100)
    dtimes = np.concatenate([-dtimes[1:][::-1], dtimes])
    dtimes = dtimes * u.s
    return dtimes


def _create_star_positions_array(star, instants):
    """
    Create an array of star positions for the GCRS frame.

    Parameters
    ----------
    star : SkyCoord
        Star coordinates.
    instants : numpy.ndarray
        Array of instants.

    Returns
    -------
    EarthLocation
        Star positions in the ITRS frame.
    """
    star_ras, star_decs = np.repeat(star.ra, len(
        instants)), np.repeat(star.dec, len(instants))
    centers_gcrs = GCRS(ra=star_ras, dec=star_decs,
                        distance=1 * u.R_earth, obstime=instants)
    centers_itrs = centers_gcrs.transform_to(ITRS(obstime=instants))
    return centers_itrs.earth_location

def _latlon_circle(latitude_c, longitude_c, radius, angle):
    """
    Calculates the new latitude and longitude coordinates given a center point, radius, and angle.

    Args:
        latitude_c (float): The latitude of the center point in degrees.
        longitude_c (float): The longitude of the center point in degrees.
        radius (float): The radius of the circle in kilometers.
        angle (float): The angle in radians.

    Returns:
        tuple: A tuple containing the new latitude and longitude coordinates in degrees.
    """
    # Calculate the angular variations
    delta_phi = radius / 6371.0  # Assuming the mean Earth radius of 6371 km
    delta_lambda = delta_phi / np.cos(np.radians(latitude_c))

    # Calculate the new coordinates
    new_latitude = np.degrees(np.arcsin(np.sin(np.radians(latitude_c)) * np.cos(delta_phi) + np.cos(np.radians(latitude_c)) * np.sin(delta_phi) * np.cos(angle)))
    new_longitude = longitude_c + np.degrees(delta_lambda * np.sin(angle))

    return new_latitude, new_longitude

def _check_nighttime(location, instant):
    """
    Check if the occultation happens at nighttime.

    Parameters
    ----------
    location : `~astropy.coordinates.EarthLocation`
        The geographic location of the observer.
    instant : `~astropy.time.Time`
        The time at which the occultation event occurs.

    Returns
    -------
    bool
        True if the occultation event occurs during nighttime, False otherwise.
    """
    try:
        sun = get_sun(instant)
        sun_lat = sun.dec
        sun_lon = sun.ra - instant.sidereal_time('mean', 'greenwich')
        sun_theta = np.arccos(np.sin(location.lat)*np.sin(sun_lat) + np.cos(
            location.lat)*np.cos(sun_lat)*np.cos(abs(location.lon-sun_lon)))
        return any(sun_theta.to_value('deg') > 89.47) # 90 deg from sun dist - 0.53 deg from sun apparent size
    except:
        return True


def _path_arc(location, path_location):
    """
    Calculate the linear distance between two locations on the Earth's surface.

    Parameters:
    location (EarthLocation): An EarthLocation object representing the observer's location on the Earth's surface.
    path_location (EarthLocation): An EarthLocation object representing the location of a point on the Earth's surface.

    Returns:
    path_arc (Quantity): The linear distance between the observer's location and the point on the Earth's surface, in kilometers.
    """

    path_theta = np.arccos(np.sin(location.lat)*np.sin(path_location.lat) + np.cos(
        location.lat)*np.cos(path_location.lat)*np.cos(abs(location.lon-path_location.lon)))
    return path_theta.value * const.R_earth.to_value(u.km) * u.km


def _calculate_path_visibility(location, path, radius, latitudinal=False, additional_path=None, ext_radius=0):
    """
    Calculate if the central path is within range.

    Parameters
    ----------
    location : EarthLocation
        The observer's location on Earth's surface.
    path : list
        The longitude and latitude of the path.
    radius : Quantity
        The radius of visibility around the observer's location.
    latitudinal : bool, optional
        If True, calculate the distance between the observer's longitude and the path's longitude. 
        If any of the distances are greater than the radius, return True. 
        If False, check if there is an additional path (e.g., ring or body limits).
    additional_path : list, optional
        The longitude and latitude of an additional path.
    ext_radius : int, optional
        The radius of the additional path.

    Returns
    -------
    bool
        True if the path is visible, False otherwise.
    """

    path_location = EarthLocation.from_geodetic(
        lat=path[1]*u.deg, lon=path[0]*u.deg, height=0*u.m)

    if latitudinal:
        path_distances = abs(
            path_location.lon - location.lon).to_value(u.rad) * const.R_earth.to_value('km') * u.km
        return len(path_distances) > 0 and any(path_distances > radius)
    else:
        if additional_path is not None:
            add_path_location = EarthLocation.from_geodetic(
                lat=additional_path[1]*u.deg, lon=additional_path[0]*u.deg, height=0*u.m)
            add_path_arc = _path_arc(location, add_path_location)
            add_arc = add_path_arc.min()
            if len(add_path_arc) > 0 and (add_arc <= radius):
                return True
            else:
                path_arc = _path_arc(location, path_location)
                tot_arc = add_arc + path_arc.min()
                return len(path_arc) > 0 and (tot_arc <= (ext_radius*u.km + radius))
        else:
            path_arc = _path_arc(location, path_location)
            return len(path_arc) > 0 and (path_arc.min() <= radius)


def _polynomial_fit(x, y, degree):
    """
    Perform a polynomial fit on a set of data points.

    Args:
        x (list): The x-coordinates of the data points.
        y (list): The y-coordinates of the data points.
        degree (int): The degree of the polynomial fit.

    Returns:
        ndarray: The coefficients of the polynomial fit.
    """
    return np.polyfit(x, y, degree)


def _path_latlon_coeff(instants, central_instant, dtimes, centers, delta, ca, vel, pa, pa_plus, radius=0, degree=19):
    '''Private function that calculates the latitude and longitude values for a given path based on the coefficients of a polynomial fit.

    Args:
        instants (list): A list of astropy `Time` objects representing the time instants.
        dtimes (list): A list of time intervals.
        centers (SkyCoord): A `SkyCoord` object representing the center coordinates.
        delta (Quantity): The angular separation between two points on the sky.
        ca (Quantity): The chord length between the two points.
        vel (Quantity): The velocity of the object.
        pa (Quantity): The position angle of the chord.
        pa_plus (Quantity): The position angle plus 90 degrees.
        radius (Quantity, optional): The radius of the object. Defaults to 0.
        degree (int, optional): The degree of the polynomial fit. Defaults to 19.

    Returns:
        tuple: A tuple containing the longitude and latitude coefficients, as well as the maximum and minimum longitude values.
            lon_coeff (ndarray): The coefficients of the polynomial fit for the longitude values.
            lat_coeff (ndarray): The coefficients of the polynomial fit for the latitude values.
            lon_max (float): The maximum longitude value.
            lon_min (float): The minimum longitude value.
            nightside (bool): True if any part of the path is at earth's nightside
    '''

    arc_x, arc_y = _calculate_arc_coordinates(delta, ca, pa, dtimes, vel, pa_plus, radius)

    lon, lat = _xy2latlon(arc_x, arc_y, centers.lon.value, centers.lat.value, instants)

    valid_coordinates = (lon < 1e+31)
    lon, lat, times = lon[valid_coordinates], lat[valid_coordinates], dtimes[valid_coordinates] - dtimes[0]
    if (len(lon) > degree+1) and (len(lat) > degree+1):
        try:
            # check if it happens at nighttime
            location = EarthLocation.from_geodetic(lat=lat*u.deg, lon=lon*u.deg, height=0*u.m)
            nighttime = _check_nighttime(location, central_instant)
            lon = _handle_longitude_discontinuities(lon)
            lon_coeff = _polynomial_fit(times, lon, degree)
            lat_coeff = _polynomial_fit(times, lat, degree)
            return lon_coeff.tolist(), lat_coeff.tolist(), lon.max(), lon.min(), lat.max(), lat.min(), nighttime
        except:
            return [], [], None, None, None, None, False
    else:
        return [], [], None, None, None, None, False


def _build_path_from_coeff(lon_coeff, lat_coeff, t0, t1, n_elements):
    """
    Calculate the longitude and latitude values for each element in the given time range using the provided coefficients.

    Args:
        lon_coeff (list): Coefficients for longitude calculation.
        lat_coeff (list): Coefficients for latitude calculation.
        t0 (astropy.time.Time): Start time.
        t1 (astropy.time.Time): End time.
        n_elements (int): Number of elements to calculate within the time range.

    Returns:
        tuple: A tuple containing the arrays of longitude and latitude values.

    Example:
        lon_coeff = [1, 2, 3]
        lat_coeff = [4, 5, 6]
        t0 = Time('2022-01-01')
        t1 = Time('2022-01-02')
        n_elements = 100

        path = _build_path_from_coeff(lon_coeff, lat_coeff, t0, t1, n_elements)
        print(path)
    """
    if isinstance(lon_coeff, list) == False or isinstance(lat_coeff, list) == False:
        return None

    try:
        t0 = Time(datetime.fromisoformat(t0).replace(
            tzinfo=None), format='datetime', scale='utc')
        t1 = Time(datetime.fromisoformat(t1).replace(
            tzinfo=None), format='datetime', scale='utc')

        degree = len(lon_coeff) - 1
        times = np.linspace(0, (t1-t0).value*86400, n_elements)

        latitude = np.zeros(len(times))
        longitude = np.zeros(len(times))

        for i in range(len(lon_coeff)):
            latitude += lat_coeff[i] * times**(degree-i)
            longitude += lon_coeff[i] * times**(degree-i)

        return longitude, latitude
    except Exception as e:
        print(e)
        return None


def occultation_path(date_time, star_coordinates, closest_approach, position_angle, velocity, delta_distance, offset=[0, 0], object_radius=None, ring_radius=None, interpolate=True):
    '''
    Returns the occultation paths, and upper and lower limits when object and/or ring radius is provided.

    Args:
        date_time (str): The date and time of the observation in the format 'YYYY-MM-DDTHH:MM:SS'.
        star_coordinates (tuple): The coordinates of the star in the format (RA, Dec) where RA is in hours and Dec is in degrees.
        closest_approach (float): The closest approach of the object to the observer in arcseconds.
        position_angle (float): The position angle of the object relative to the observer in degrees.
        velocity (float): The velocity of the object relative to the observer in kilometers per second.
        delta_distance (float): The distance of the object from the observer in astronomical units (AU).
        offset (list, optional): The offset of the observer from the center of the object in milliarcseconds (mas). Default is [0, 0].
        object_radius (float, optional): The radius of the object in kilometers. Default is None.
        ring_radius (float, optional): The radius of the ring in kilometers. Default is None.
        interpolate (bool, optional): A boolean flag indicating whether to interpolate the coordinates. Default is True.

    Returns:
        list: The occultation path coordinates.
        list: The upper and lower limits for the occultation path when object radius is provided.
        list: The upper and lower limits for the occultation path when ring radius is provided.

    This code borrows heavily from SORA.
    '''
    instant, star, delta, vel, pa, ca = _setup_initial_variables(
        date_time, star_coordinates, delta_distance, velocity, position_angle, closest_approach, offset)
    pa_plus = _calculate_position_angle(pa)
    dtimes = _generate_instants_array(vel)
    centers = _create_star_positions_array(star, instant + dtimes)
    instants = dtimes + instant
    upper_limit, lower_limit, ring_upper_limit, ring_lower_limit = None, None, None, None
    path = _path_latlon(instants, dtimes, centers, delta, ca,
                        vel, pa, pa_plus, radius=0, interpolate=interpolate)

    if object_radius is not None:
        upper_limit = _path_latlon(instants, dtimes, centers, delta, ca,
                                   vel, pa, pa_plus, radius=object_radius, interpolate=interpolate)
        lower_limit = _path_latlon(instants, dtimes, centers, delta, ca,
                                   vel, pa, pa_plus, radius=-object_radius, interpolate=interpolate)

    if ring_radius is not None:
        ring_upper_limit = _path_latlon(
            instants, dtimes, centers, delta, ca, vel, pa, pa_plus, radius=ring_radius, interpolate=interpolate)
        ring_lower_limit = _path_latlon(
            instants, dtimes, centers, delta, ca, vel, pa, pa_plus, radius=-ring_radius, interpolate=interpolate)

    return [path, [upper_limit, lower_limit], [ring_upper_limit, ring_lower_limit]]


def visibility(latitude, longitude, radius, date_time, star_coordinates, closest_approach, position_angle, velocity, delta_distance, offset=[0, 0], object_radius=None, ring_radius=None, latitudinal=False, interpolate=True):
    '''
    Computes the visibility of an occultation event given the latitude, longitude, and radius of a location.

    Args:
        latitude (float): The latitude of the location in degrees.
        longitude (float): The longitude of the location in degrees.
        radius (float): The radius around the location in kilometers.
        date_time (str): The date and time of the observation in the format 'YYYY-MM-DDTHH:MM:SS'.
        star_coordinates (tuple): The coordinates of the star in the format (RA, Dec) where RA is in hours and Dec is in degrees.
        closest_approach (float): The closest approach of the object to the observer in arcseconds.
        position_angle (float): The position angle of the object relative to the observer in degrees.
        velocity (float): The velocity of the object relative to the observer in kilometers per second.
        delta_distance (float): The distance of the object from the observer in astronomical units (AU).
        offset (list, optional): The offset of the observer from the center of the object in milliarcseconds (mas). Default is [0, 0].
        object_radius (float, optional): The radius of the object in kilometers. Default is None.
        ring_radius (float, optional): The radius of the ring in kilometers. Default is None.
        latitudinal (bool, optional): If True, calculate the distance between the observer's longitude and the path's longitude. If False, check if there is an additional path (e.g., ring or body limits).
        interpolate (bool, optional): A boolean flag indicating whether to interpolate the coordinates. Default is True.

    Returns:
        visibility (bool): True if the occultation event is visible, False otherwise.
        info (str): Additional information about the visibility.
    '''

    info = ''
    kstr = ''

    latitudes, longitudes = _latlon_circle(latitude, longitude, radius, np.arange(0,360,0.1))

    location_c = EarthLocation.from_geodetic(
        lat=latitudes*u.deg, lon=longitudes*u.deg, height=0*u.m)
    
    location = EarthLocation.from_geodetic(
        lat=latitude*u.deg, lon=longitude*u.deg, height=0*u.m)

    radius = radius * u.km
    nighttime = _check_nighttime(location_c, Time(
        date_time))

    path, object_limits, ring_limits = occultation_path(date_time, star_coordinates, closest_approach, position_angle, velocity,
                                                        delta_distance, offset=offset, object_radius=object_radius, ring_radius=ring_radius, interpolate=interpolate)

    # check if the ring is passing over the location
    upper_ring_visibility, lower_ring_visibility = False, False
    if ring_limits[0] is not None:
        upper_ring_visibility = _calculate_path_visibility(
            location, path, radius, latitudinal=latitudinal, additional_path=ring_limits[0], ext_radius=ring_radius)
    if ring_limits[1] is not None:
        lower_ring_visibility = _calculate_path_visibility(
            location, path, radius, latitudinal=latitudinal, additional_path=ring_limits[1], ext_radius=ring_radius)
    if upper_ring_visibility or lower_ring_visibility:
        info += "Body's ring(s) shadow pass within selected area."
        kstr = ' '

    # check if the object is passing over the location
    obj_upperlim_visibility, obj_lowerlim_visibility = False, False
    if object_limits[0] is not None:
        obj_upperlim_visibility = _calculate_path_visibility(
            location, path, radius, latitudinal=latitudinal, additional_path=object_limits[0], ext_radius=object_radius)
    if object_limits[1] is not None:
        obj_lowerlim_visibility = _calculate_path_visibility(
            location, path, radius, latitudinal=latitudinal, additional_path=object_limits[1], ext_radius=object_radius)
    if obj_upperlim_visibility or obj_lowerlim_visibility:
        info += kstr+'Main body shadow passes within selected area.'

    # if the object has a radius and passes over the location there is no need to compute the central path,
    # however, if there is no radius information, check if the central path passes over the location
    path_visibility = False
    if not (obj_upperlim_visibility and obj_lowerlim_visibility):
        path_visibility = _calculate_path_visibility(
            location, path, radius, latitudinal=latitudinal)

    visibility = np.logical_or.reduce(np.array(
        [path_visibility, obj_upperlim_visibility, obj_lowerlim_visibility, upper_ring_visibility, lower_ring_visibility]))

    return np.logical_and(nighttime, visibility), info

def occultation_path_coeff(
        date_time: Union[datetime, str],
        ra_star_candidate: str,
        dec_star_candidate: str,
        closest_approach: float,
        position_angle: float,
        velocity: float,
        delta_distance: float,
        offset_ra: float,
        offset_dec: float,
        object_diameter: Optional[float] = None,
        ring_radius: Optional[float] = None,
        degree: float = 19):
    """
        Args:
        date_time (Union[datetime, str]): The date and time of the observation in the format 'YYYY-MM-DDTHH:MM:SS'.
        ra_star_candidate (str): The right ascension of the star candidate in the format 'HH:MM:SS'.
        dec_star_candidate (str): The declination of the star candidate in the format 'DD:MM:SS'.
        closest_approach (float): The closest approach of the object to the observer in arcseconds.
        position_angle (float): The position angle of the object relative to the observer in degrees.
        velocity (float): The velocity of the object relative to the observer in kilometers per second (km/s).
        delta_distance (float): The distance of the object from the observer in astronomical units (AU).
        offset_ra (float): The offset of the observer from the center of the object in right ascension in milliarcseconds (mas).
        offset_dec (float): The offset of the observer from the center of the object in declination in milliarcseconds (mas).
        object_diameter (float, optional): The diameter of the object in km. Default is None.
        ring_radius (float, optional): The radius of the ring around the object in degrees. Default is None.
        degree (float, optional): The degree of the polynomial fit. Default is 19.

    Returns:
        dict: A dictionary containing the calculated coefficients and other relevant information.
            - 't0' (str): The initial instant of the occultation path in ISO format.
            - 't1' (str): The final instant of the occultation path in ISO format.
            - 'coeff_latitude' (list): The coefficients of the polynomial fit for the latitude values of the occultation path.
            - 'coeff_longitude' (list): The coefficients of the polynomial fit for the longitude values of the occultation path.
            - 'body_upper_coeff_latitude' (list): The coefficients of the polynomial fit for the latitude values of the upper limit of the object.
            - 'body_upper_coeff_longitude' (list): The coefficients of the polynomial fit for the longitude values of the upper limit of the object.
            - 'body_lower_coeff_latitude' (list): The coefficients of the polynomial fit for the latitude values of the lower limit of the object.
            - 'body_lower_coeff_longitude' (list): The coefficients of the polynomial fit for the longitude values of the lower limit of the object.
            - 'ring_upper_coeff_latitude' (list): The coefficients of the polynomial fit for the latitude values of the upper limit of the ring.
            - 'ring_upper_coeff_longitude' (list): The coefficients of the polynomial fit for the longitude values of the upper limit of the ring.
            - 'ring_lower_coeff_latitude' (list): The coefficients of the polynomial fit for the latitude values of the lower limit of the ring.
            - 'ring_lower_coeff_longitude' (list): The coefficients of the polynomial fit for the longitude values of the lower limit of the ring.
            - 'min_longitude' (float): The minimum longitude value from the calculated coefficients.
            - 'max_longitude' (float): The maximum longitude value from the calculated coefficients.
            - 'min_latitude' (float): The minimum latitude value from the calculated coefficients.
            - 'max_latitude' (float): The maximum latitude value from the calculated coefficients.
            - 'nightside' (bool): True if the object is on the nightside of the observer, False otherwise.
    """
    if isinstance(date_time, str):
        date_time = datetime.fromisoformat(date_time)
    date_time = date_time.isoformat().replace("+00:00", "Z")
    
    star_coordinates = f"{ra_star_candidate} {dec_star_candidate}"
    offset = (offset_ra, offset_dec)

    object_radius = float(
        object_diameter / 2) if object_diameter != None else None

    instant, star, delta, vel, pa, ca = _setup_initial_variables(
        date_time, star_coordinates, delta_distance, velocity, position_angle, closest_approach, offset)
    
    pa_plus = _calculate_position_angle(pa)
    dtimes = _generate_instants_array(vel)
    centers = _create_star_positions_array(star, instant + dtimes)
    instants = dtimes + instant
    upper_limit, lower_limit, ring_upper_limit, ring_lower_limit = None, None, None, None

    lons, lats, nightside = [], [], []

    output = {
        't0': None,
        't1': None,
        'coeff_latitude': [],
        'coeff_longitude': [],
        'body_upper_coeff_latitude': [],
        'body_upper_coeff_longitude': [],
        'body_lower_coeff_latitude': [],
        'body_lower_coeff_longitude': [],
        'ring_upper_coeff_latitude': [],
        'ring_upper_coeff_longitude': [],
        'ring_lower_coeff_latitude': [],
        'ring_lower_coeff_longitude': [],
        'min_longitude': None,
        'max_longitude': None,
        'min_latitude': None,
        'max_latitude': None,
        'nightside': False
    }

    output.update({
        't0': instants[0].datetime.astimezone(tz=timezone.utc).isoformat(),
        't1': instants[-1].datetime.astimezone(tz=timezone.utc).isoformat()
    })

    result = _path_latlon_coeff(
        instants, instant, dtimes, centers, delta, ca, vel, pa, pa_plus, radius=0, degree=degree)

    output.update({
        'coeff_latitude': result[1],
        'coeff_longitude': result[0]
    })
    lons.append([result[2], result[3]])
    lats.append([result[4], result[5]])

    nightside.append([result[6]])

    if object_radius is not None:
        upper_limit = _path_latlon_coeff(
            instants, instant, dtimes, centers, delta, ca, vel, pa, pa_plus, radius=object_radius, degree=degree)
        output.update({
            'body_upper_coeff_latitude': upper_limit[1],
            'body_upper_coeff_longitude': upper_limit[0]
        })
        lower_limit = _path_latlon_coeff(
            instants, instant, dtimes, centers, delta, ca, vel, pa, pa_plus, radius=-object_radius, degree=degree)
        output.update({
            'body_lower_coeff_latitude': lower_limit[1],
            'body_lower_coeff_longitude': lower_limit[0]
        })
        lons.append([upper_limit[2], upper_limit[3],
                     lower_limit[2], lower_limit[3]])
        lats.append([upper_limit[4], upper_limit[5],
                     lower_limit[4], lower_limit[5]])
        nightside.append([upper_limit[6], lower_limit[6]])

    if ring_radius is not None:
        ring_upper_limit = _path_latlon_coeff(
            instants, instant, dtimes, centers, delta, ca, vel, pa, pa_plus, radius=ring_radius, degree=degree)
        output.update({
            'ring_upper_coeff_latitude': ring_upper_limit[1],
            'ring_upper_coeff_longitude': ring_upper_limit[0]
        })
        ring_lower_limit = _path_latlon_coeff(
            instants, instant, dtimes, centers, delta, ca, vel, pa, pa_plus, radius=-ring_radius, degree=degree)
        output.update({
            'ring_lower_coeff_latitude': ring_lower_limit[1],
            'ring_lower_coeff_longitude': ring_lower_limit[0]
        })
        lons.append([ring_upper_limit[2], ring_upper_limit[3],
                     ring_lower_limit[2], ring_lower_limit[3]])
        lats.append([ring_upper_limit[4], ring_upper_limit[5],
                     ring_lower_limit[4], ring_lower_limit[5]])
        nightside.append([ring_upper_limit[6], ring_lower_limit[6]])

    longitudes = np.array(
        [item for sublist in lons if sublist is not None for item in sublist if item is not None])
    index = np.where(longitudes > 180)
    longitudes[index] -= 360

    latitudes = np.array(
        [item for sublist in lats if sublist is not None for item in sublist if item is not None])
    
    try:
        output.update({'min_longitude': longitudes.min(),
                       'max_longitude': longitudes.max(),
                       'min_latitude': latitudes.min(),
                       'max_latitude': latitudes.max()
                       })
    except:
        output.update({'min_longitude': None,
                       'max_longitude': None,
                       'min_latitude': None,
                       'max_latitude': None  
                       })

    nightsides = np.array(
        [item for sublist in nightside if sublist is not None for item in sublist if item is not None])

    output.update({'nightside': any(nightsides)})

    return output


def visibility_from_coeff(
    latitude: float,
    longitude: float,
    radius: float,
    date_time: Union[datetime, str],
    inputdict: Union[dict, str],
    n_elements: int = 1500,
    object_diameter: Optional[float] = None,
    ring_radius: Optional[float] = None,
    latitudinal: bool = False
):
    '''
    Computes the visibility of an occultation event given its latitude, longitude, and radius around a specific location.

    Parameters:
    latitude (float): The latitude of the observer's location in degrees.
    longitude (float): The longitude of the observer's location in degrees.
    radius (float): The radius around the observer's location in kilometers.
    date_time (str): The date and time of the occultation event in the format 'YYYY-MM-DD HH:MM:SS'.
    inputdict (dict): A dictionary containing the coefficients and time range for the central path, body limits, and ring limits.
    n_elements (int, optional): The number of elements to calculate within the time range. Defaults to 500.
    object_radius (float, optional): The radius of the object (e.g., planet or moon) in kilometers. Defaults to None.
    ring_radius (float, optional): The radius of the ring (if applicable) in kilometers. Defaults to None.
    latitudinal (bool, optional): A flag indicating whether to calculate the distance between the observer's longitude and the path's longitude. Defaults to False.
    interpolate (bool, optional): A flag indicating whether to interpolate the longitude and latitude values for each element in the time range. Defaults to True.

    Returns:
    tuple: A tuple containing the visibility status (boolean) and additional information about the visibility conditions (string).
    '''
    if isinstance(date_time, str):
        date_time = datetime.fromisoformat(date_time)
    date_time = date_time.isoformat().replace("+00:00", "Z")

    if isinstance(inputdict, str):
        inputdict = json.loads(inputdict)

    object_radius = float(
        object_diameter / 2) if object_diameter != None else None

    info = ''
    kstr = ''

    latitudes, longitudes = _latlon_circle(latitude, longitude, radius, np.arange(0,360,0.1))

    location_c = EarthLocation.from_geodetic(
        lat=latitudes*u.deg, lon=longitudes*u.deg, height=0*u.m)
    
    location = EarthLocation.from_geodetic(
        lat=latitude*u.deg, lon=longitude*u.deg, height=0*u.m)
    
    radius = radius * u.km

    nighttime = _check_nighttime(location_c, Time(date_time))
    
    path = _build_path_from_coeff(
        inputdict['coeff_longitude'], inputdict['coeff_latitude'], inputdict['t0'], inputdict['t1'], n_elements)

    object_upper_limit = _build_path_from_coeff(inputdict['body_upper_coeff_longitude'], inputdict['body_upper_coeff_latitude'],
                                                inputdict['t0'], inputdict['t1'], n_elements) if 'body_upper_coeff_longitude' in inputdict else None
    object_lower_limit = _build_path_from_coeff(inputdict['body_lower_coeff_longitude'], inputdict['body_lower_coeff_latitude'],
                                                inputdict['t0'], inputdict['t1'], n_elements) if 'body_lower_coeff_longitude' in inputdict else None
    object_limits = [object_upper_limit, object_lower_limit]

    ring_upper_limit = _build_path_from_coeff(inputdict['ring_upper_coeff_longitude'], inputdict['ring_upper_coeff_latitude'],
                                              inputdict['t0'], inputdict['t1'], n_elements) if 'ring_upper_coeff_longitude' in inputdict else None
    ring_lower_limit = _build_path_from_coeff(inputdict['ring_lower_coeff_longitude'], inputdict['ring_lower_coeff_latitude'],
                                              inputdict['t0'], inputdict['t1'], n_elements) if 'ring_lower_coeff_longitude' in inputdict else None
    ring_limits = [ring_upper_limit, ring_lower_limit]

    # check if the ring is passing over the location
    upper_ring_visibility, lower_ring_visibility = False, False
    if (ring_limits[0] is not None) and (ring_radius is not None):
        upper_ring_visibility = _calculate_path_visibility(
            location, path, radius, latitudinal=latitudinal, additional_path=ring_limits[0], ext_radius=ring_radius)
    if (ring_limits[1] is not None) and (ring_radius is not None):
        lower_ring_visibility = _calculate_path_visibility(
            location, path, radius, latitudinal=latitudinal, additional_path=ring_limits[1], ext_radius=ring_radius)
    if upper_ring_visibility or lower_ring_visibility:
        info += "Body's ring(s) shadow pass within selected area."
        kstr = ' '

    # check if the object is passing over the location
    obj_upperlim_visibility, obj_lowerlim_visibility = False, False
    if (object_limits[0] is not None) and (object_radius is not None):
        obj_upperlim_visibility = _calculate_path_visibility(
            location, path, radius, latitudinal=latitudinal, additional_path=object_limits[0], ext_radius=object_radius)
    if (object_limits[1] is not None) and (object_radius is not None):
        obj_lowerlim_visibility = _calculate_path_visibility(
            location, path, radius, latitudinal=latitudinal, additional_path=object_limits[1], ext_radius=object_radius)
    if obj_upperlim_visibility or obj_lowerlim_visibility:
        info += kstr+'Main body shadow passes within selected area.'

    # if the object has a radius and passes over the location there is no need to compute the central path,
    # however, if there is no radius information, check if the central path passes over the location
    path_visibility = False
    try:
        if not (obj_upperlim_visibility and obj_lowerlim_visibility):
            path_visibility = _calculate_path_visibility(
                location, path, radius, latitudinal=latitudinal)
    except:
        pass

    visibility = np.logical_or.reduce(np.array(
        [path_visibility, obj_upperlim_visibility, obj_lowerlim_visibility, upper_ring_visibility, lower_ring_visibility]))

    return bool(np.logical_and(nighttime, visibility)), info
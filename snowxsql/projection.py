'''
Module for functions that handle anything regarding coordinate projections.
'''
import utm
from geoalchemy2.elements import WKTElement
import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling
from rasterio.crs import CRS

def reproject_point_in_dict(info, is_northern=True):
    '''
    Searches the info dictionary and converts from lat long to northing easting
    and vice versa if either are missing.

    Args:
        info: Dictionary containing key northing/easting or latitude longitude
        is_northern: Boolean for which hemisphere this data is in

    Returns:
        result: Dictionary containing all previous information plus a coordinates
                reprojected counter part
    '''
    result = info.copy()
    keys = result.keys()

    # Convert any coords to numbers
    for c in ['northing','easting','latitude','longitude']:
        if c in result.keys():
            result[c] = float(result[c])

    # Convert UTM coordinates to Lat long or vice versa for database storage
    if 'northing' in keys:

        if type(result['utm_zone']) == str:
                result['utm_zone'] = \
                   int(''.join([s for s in result['utm_zone'] if s.isnumeric()]))

        lat, long = utm.to_latlon(result['easting'], result['northing'],
                                                     result['utm_zone'],
                                                     northern=is_northern)

        result['latitude'] = lat
        result['longitude'] = long

    elif 'latitude' in keys:
        easting, northing, utm_zone, letter = utm.from_latlon(
                                            result['latitude'],
                                            result['longitude'])
        result['easting'] = easting
        result['northing'] = northing
        result['utm_zone'] = utm_zone

    return result


def add_geom(info, epsg):
    '''
    Adds the WKBElement to the dictionary

    Args:
        info: Dictionary containing easting and northing keys
        epsg: integer representing the projection code

    Returns:
        geom: WKTElement value
    '''
    # Add a geometry entry
    geom = WKTElement('SRID={}; POINT({} {})'
                        ''.format(epsg, info['easting'], info['northing']),
                                 extended=True)
    return geom


def reproject_raster_by_epsg(input_f, output_f, epsg):
    '''
    Reproject a geotiff raster from one epsg to another

    Args:
        input_f: Input path to a geotiff
        output_f: Output  location of a reprojected geotiff
        epsg: Valid projection reference number
    '''

    dst_crs = 'EPSG:{}'.format(epsg)

    with rasterio.open(input_f) as src:
        transform, width, height = calculate_default_transform(
            src.crs, dst_crs, src.width, src.height, *src.bounds)
        kwargs = src.meta.copy()
        kwargs.update({
            'crs': dst_crs,
            'transform': transform,
            'width': width,
            'height': height
        })

        with rasterio.open(output_f, 'w', **kwargs) as dst:
            for i in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, i),
                    destination=rasterio.band(dst, i),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=transform,
                    dst_crs=dst_crs,
                    resampling=Resampling.bilinear)

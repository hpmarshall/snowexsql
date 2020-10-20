'''
'''

from snowxsql.data import  *
from snowxsql.conversions import query_to_geopandas
from snowxsql.db import get_db
from snowxsql.utilities import get_logger
from snowxsql.analysis import *
import geoalchemy2.functions as gfunc
from geoalchemy2.shape import to_shape, from_shape
from sqlalchemy.sql import func
from shapely.geometry import Polygon, Point
from geoalchemy2.types import Raster
import geopandas as gpd
import progressbar
import matplotlib.pyplot as plt

def get_raster_value(session, point, surveyor, date=None):
    '''
    Grab the raster tiles that fall into the polygon. Resample. Attempt to get
    raster value
    '''

    #s = func.ST_Clip(func.ST_Rescale(gfunc.ST_Union(ImageData.raster, type_=Raster), res, -1 * res, 'Bilinear'), shp, True)

    # Filter by surveyors and data name DEM
    q = session.query(func.ST_Value(ImageData.raster, 1, point)).filter(ImageData.type == 'DEM')
    q = q.filter(ImageData.surveyors == surveyor)

    if date != None:
        q = q.filter(ImageData.date == date)

    # Grab all tile that intersect the snow on boundary
    # q = q.filter(gfunc.ST_Intersects(ImageData.raster, shp))

    q = q.filter(gfunc.ST_Within(point, func.ST_Envelope(ImageData.raster)))
    value = q.all()

    return value[0][0]

def main():

    # Grab the db session
    engine, session = get_db('snowex')

    surveyors = ['aso','usgs']

    #Setup
    log = get_logger('Depths Script')
    # Get the count of QSI dates
    dates = session.query(ImageData.date).filter(ImageData.surveyors=='QSI').distinct().all()

    # Build an empy dataframe fro storing our results in
    results = gpd.GeoDataFrame(columns=['geometry','aso','usgs', 'measured','date'])

    # Grab all depths and dates.
    q = session.query(PointData)
    q = q.filter(PointData.type=='depth').limit(50)
    df = query_to_geopandas(q, engine)
    log.info('Found {} snow depths...'.format(len(df)))

    bar = progressbar.ProgressBar(max_value=len(df.index))

    # Loop over all the points
    for i,row in df.iterrows():

        # Create an empty dict and add geometryand date for each point
        data = {}
        data['measured'] = row['value']
        data['geometry'] = row['geom']
        data['date'] = row['date']

        point = from_shape(row['geom'], srid=26912).ST_AsEWKT()


        # Get the raster value of a cell nearest center after resampling to the resolution
        snow = get_raster_value(session, point, 'QSI', date=dates[0][0])

        for surveyor in surveyors:
            off = get_raster_value(session, point,  surveyor.upper())

            if off == None or snow == None:
                data[surveyor] = None
            else:
                data[surveyor] = (snow - off) * 100 # cm

        results = results.append(data, ignore_index=True)
        bar.update(i)

    session.close()

    log.info('Differences:')

    # Calculate the differences
    for n in surveyors:
        log.info(n)
        name = '{} diff'.format(n)
        results[name] = results[n] - results['measured']

        # report the stats
        get_stats(results[name], logger=log)

    fig, ax = plt.subplots(1, 1, figsize = (8,8))

    # Plot the points colored by differences
    results.plot(ax=ax, column='usgs diff', cmap='RdBu', vmin=-50, vmax=50, legend=True)

    # Don't use scientific notation on the axis ticks
    ax.ticklabel_format(style='plain', useOffset=False)

    # Add x/y labels, a title, a legend and avoid overlapping labels
    ax.set_xlabel('Easting [m]')
    ax.set_ylabel('Northing [m]')
    ax.set_title('USGS')
    plt.show()

if __name__ == '__main__':
    main()

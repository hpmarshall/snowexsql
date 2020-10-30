from sqlalchemy import MetaData
import datetime
import numpy as np

from os import remove
from os.path import join, dirname

from snowxsql.upload import *
from snowxsql.functions import ST_PixelAsPoint
from . sql_test_base import DBSetup

from shapely.geometry import Point
from geoalchemy2.shape import to_shape
from geoalchemy2.elements import WKTElement
from geoalchemy2.types import Raster

from snowxsql.conversions import raster_to_rasterio

from  .sql_test_base import DBSetup, TableTestBase, pytest_generate_tests
from rasterio.plot import show

class TestRaster(TableTestBase):
    '''
    Test uploading a raster
    '''

    # Class to use to upload the data
    UploaderClass = UploadRaster

    # Positional arguments to pass to the uploader class
    args = [join('be_gm1_0328','w001001x.adf')]

    # Keyword args to pass to the uploader class
    kwargs = {'type':'dem', 'epsg':26912, 'description':'test'}

    # Always define this using a table class from data.py and is used for ORM
    TableClass = ImageData

    # First filter to be applied is count_attribute == data_name
    count_attribute = 'type'


    # Define params which is a dictionary of test names and their args
    params = {
    'test_count': [dict(data_name='dem', expected_count=1)],
    'test_value': [dict(data_name='dem', attribute_to_check='description', filter_attribute='id', filter_value=1, expected='test')],
    'test_unique_count': [dict(data_name='dem', attribute_to_count='id', expected_count=1)]
            }

class TestTiledRaster(DBSetup):
    '''
    A class to test common operations and features of tiled raster in the DB
    '''

    def setup_class(self):
        '''
        Setup the database one time for testing
        '''
        super().setup_class()
        # Positional arguments to pass to the uploader class
        args = [join(self.data_dir, 'uavsar','uavsar_utm.amp1.real.tif')]

        # Keyword args to pass to the uploader class
        kwargs = {'type':'insar', 'epsg':26912, 'description':'test', 'tiled':True}
        # Upload two rasters (next two each other)
        u = UploadRaster(*args, **kwargs)
        u.submit(self.session)

    def test_tiled_raster_count(self):
        '''
        Test two rasters uploaded
        '''
        records = self.session.query(ImageData.id).all()
        assert len(records) == 4

    def test_tiled_raster_size(self):
        '''
        Tiled raster should be 500x500 in most cases (can be smaller to fit domains)
        '''
        rasters = self.session.query(func.ST_AsTiff(ImageData.raster)).all()
        datasets = raster_to_rasterio(self.session, rasters)

        for d in datasets:
            assert d.width <= 500
            assert d.height <= 500

    def test_raster_point_retrieval(self):
        '''
        Test we can retrieve coordinates of a point from the database
        '''

        # Get the first pixel as a point
        records = self.session.query(ST_PixelAsPoint(ImageData.raster, 1, 1)).limit(1).all()
        received = to_shape(records[0][0])
        expected = Point(748446.1945536422, 4328702.971977075)

        # Convert geom to shapely object and compare
        np.testing.assert_almost_equal(received.x, expected.x, 6)
        np.testing.assert_almost_equal(received.y, expected.y, 6)

    def test_raster_union(self):
        '''
        Test we can retrieve coordinates of a point from the database
        '''

        # Get the first pixel as a point
        rasters = self.session.query(func.ST_AsTiff(func.ST_Union(ImageData.raster, type_=Raster))).all()
        assert len(rasters) == 1

    def test_raster_union2(self):
        '''
        Test we can retrieve coordinates of a point from the database
        '''

        # Get the first pixel as a point
        merged = self.session.query(func.ST_Union(ImageData.raster, type_=Raster)).filter(ImageData.id.in_([1, 2])).all()
        assert len(merged) == 1

    # def test_raster_bounds(self):
    #     '''
    #     Test the tile bounds are as expected
    #     '''
    #     rasters = self.session.query(func.ST_AsTiff(ImageData.raster)).all()
    #     datasets = raster_to_rasterio(self.session, rasters)
    #
    #     # Original untiled raster bounds from gdalinfo
    #     llx = 748446.195
    #     lly = 4328702.972
    #     urx = 751909.286
    #     ury = 4328702.972
    #
    #     for d in datasets:
    #         d.bound

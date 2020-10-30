from sqlalchemy import MetaData, inspect

from os import remove
from os.path import join, dirname

from snowxsql.upload import PointDataCSV
from snowxsql.data import PointData
from snowxsql.metadata import DataHeader
from  .sql_test_base import DBSetup, TableTestBase, pytest_generate_tests
import datetime
import pytest


class PointsBase(TableTestBase):
    args = []
    kwargs = dict(timezone='MST',
                  depth_is_metadata=False,
                  site_name='Grand Mesa',
                  epsg=26912,
                  surveyors='TEST',
                  utm_zone=12)

    TableClass = PointData
    UploaderClass = PointDataCSV


class TestSnowDepths(PointsBase):
    '''
    Class to test general behavior for the database, checking datatypes, and
    geopandas compliance. Also check that we uploaded snowdepths correctly
    '''
    args = ['depths.csv']
    params = {
    'test_count':[
            # Test that we uploaded 10 records
            dict(data_name='depth', expected_count=10)
                ],

    'test_value': [
            # Test the actual value of the dataset
            dict(data_name='depth', attribute_to_check='value', filter_attribute='id', filter_value=1, expected=94),
            # Test we rename the instrument correctly
            dict(data_name='depth', attribute_to_check='instrument', filter_attribute='id', filter_value=1, expected='magnaprobe')
            ],

    'test_unique_count': [
            # Test we have 5 unique dates
            dict(data_name='depth', attribute_to_count='date', expected_count=5)
            ]
            }

    def test_point_datatypes(self):
        '''
        Confirm that all data is stored in the correct type
        '''
        dtypes = {'id': int,
                'site_name': str,
                'date': datetime.date,
                'time': datetime.time,
                'time_created': datetime.datetime,
                'time_updated': datetime.datetime,
                'latitude': float,
                'longitude': float,
                'northing': float,
                'easting': float,
                'elevation': float,
                'utm_zone': int,
                'version_number': int,
                'type': str,
                'units': str,
                'instrument': str,
                'equipment': str,
                'value': float}

        r = self.session.query(PointData).limit(1).one()
        for c, dtype in dtypes.items():
            db_type = type(getattr(r, c))
            print(c)
            assert (db_type == dtype) or (db_type == type(None))

    def test_geopandas_compliance(self):
        '''
        Test the geometry column works
        '''
        records = self.session.query(PointData).limit(1).all()
        assert hasattr(records[0], 'geom')

    def test_extended_geom(self):
        '''
        Test that the WKBElement is assigned an SRID which is not the default
        behavior
        '''
        r = self.session.query(PointData.geom).limit(1).one()
        assert r[0].srid == 26912

class TestGPRPointData(PointsBase):
    gpr_dt =datetime.date(2019, 1, 28)

    args = ['gpr.csv']
    params = {
    'test_count':[
            # Test that we uploaded 10 records
            dict(data_name='two_way_travel', expected_count=10)
                ],

    'test_value': [
            # Test the actual value of the dataset
            dict(data_name='two_way_travel', attribute_to_check='value', filter_attribute='date', filter_value=gpr_dt, expected=8.3),
            dict(data_name='density', attribute_to_check='value', filter_attribute='date', filter_value=gpr_dt, expected=250.786035454008),
            dict(data_name='depth', attribute_to_check='value', filter_attribute='date', filter_value=gpr_dt, expected=102.662509421414),
            dict(data_name='swe', attribute_to_check='value', filter_attribute='date', filter_value=gpr_dt, expected=257.463237275561),
            # Test our unit assignment
            dict(data_name='two_way_travel', attribute_to_check='units', filter_attribute='date', filter_value=gpr_dt, expected='ns'),
            dict(data_name='density', attribute_to_check='units', filter_attribute='date', filter_value=gpr_dt, expected='kg/m^3'),
            dict(data_name='depth', attribute_to_check='units', filter_attribute='date', filter_value=gpr_dt, expected='cm'),
            dict(data_name='swe', attribute_to_check='units', filter_attribute='date', filter_value=gpr_dt, expected='mm'),
            ],

    'test_unique_count': [
            # Test we have 5 unique dates
            dict(data_name='swe', attribute_to_count='date', expected_count=3)
            ]
            }

class TestStationData(PointsBase):
    UploaderClass = PointDataCSV
    kwargs = dict(timezone='UTC',
                  depth_is_metadata=False,
                  site_name='Grand Mesa',
                  epsg=26912,
                  surveyors=None,)
                  # latitude=39.03388,
                  # longitude=-108.21399)

    args = ['met.csv']

    params = {
    'test_count':[
            # Test that we uploaded 10 records
            dict(data_name='air_temperature', expected_count=10)
                ],}

    # 'test_value': [
    #         # Test the actual value of the dataset
    #         dict(data_name='two_way_travel', attribute_to_check='value', filter_attribute='date', filter_value=gpr_dt, expected=8.3),
    #         dict(data_name='density', attribute_to_check='value', filter_attribute='date', filter_value=gpr_dt, expected=250.786035454008),
    #         dict(data_name='depth', attribute_to_check='value', filter_attribute='date', filter_value=gpr_dt, expected=102.662509421414),
    #         dict(data_name='swe', attribute_to_check='value', filter_attribute='date', filter_value=gpr_dt, expected=257.463237275561),
    #         # Test our unit assignment
    #         dict(data_name='two_way_travel', attribute_to_check='units', filter_attribute='date', filter_value=gpr_dt, expected='ns'),
    #         dict(data_name='density', attribute_to_check='units', filter_attribute='date', filter_value=gpr_dt, expected='kg/m^3'),
    #         dict(data_name='depth', attribute_to_check='units', filter_attribute='date', filter_value=gpr_dt, expected='cm'),
    #         dict(data_name='swe', attribute_to_check='units', filter_attribute='date', filter_value=gpr_dt, expected='mm'),
    #         ],
    #
    # 'test_unique_count': [
    #         # Test we have 5 unique dates
    #         dict(data_name='swe', attribute_to_count='date', expected_count=3)
    #         ]
    #         }

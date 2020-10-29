
from  .sql_test_base import pytest_generate_tests, TableTestBase
import pytest
from snowxsql.upload import UploadProfileData
from snowxsql.data import LayerData
from datetime import date, time
import pandas as pd
import pytz
import datetime
import numpy as np


class TestStratigraphyProfile(TableTestBase):
    '''
    Test that all the profiles from the Stratigraphy file were uploaded and
    have integrity
    '''

    args = ['stratigraphy.csv']
    kwargs = {'timezone':'MST'}
    UploaderClass = UploadProfileData
    TableClass = LayerData
    dt = datetime.datetime(2020, 2, 5, 13, 30, 0, 0, pytz.timezone('MST'))

    params = {
    'test_count':[dict(data_name='hand_hardness', expected_count=5)],

    # Test a value from the profile to check that the profile is there and it has integrity
    'test_value': [dict(data_name='hand_hardness', attribute_to_check='value', filter_attribute='depth', filter_value=30, expected='4F'),
                   dict(data_name='grain_size', attribute_to_check='value', filter_attribute='depth', filter_value=35, expected='< 1 mm'),
                   dict(data_name='grain_type', attribute_to_check='value', filter_attribute='depth', filter_value=17, expected='FC'),
                   dict(data_name='manual_wetness', attribute_to_check='value', filter_attribute='depth', filter_value=17, expected='D'),

                   # Test were are uploading most of the important attributes
                   dict(data_name='hand_hardness', attribute_to_check='site_id', filter_attribute='depth', filter_value=30, expected='1N20'),
                   dict(data_name='hand_hardness', attribute_to_check='date', filter_attribute='depth', filter_value=30, expected=dt.date()),
                   dict(data_name='hand_hardness', attribute_to_check='time', filter_attribute='depth', filter_value=30, expected=dt.timetz()),
                   dict(data_name='hand_hardness', attribute_to_check='site_name', filter_attribute='depth', filter_value=30, expected='Grand Mesa'),
                   dict(data_name='hand_hardness', attribute_to_check='easting', filter_attribute='depth', filter_value=30, expected=743281),
                   dict(data_name='hand_hardness', attribute_to_check='northing', filter_attribute='depth', filter_value=30, expected=4324005),

                   # Test the single comment was used
                   dict(data_name='hand_hardness', attribute_to_check='comments', filter_attribute='depth', filter_value=17, expected='Cups'),

                   ],

    'test_unique_count': [
                    # Test only 1 value was submitted to each layer for wetness
                    dict(data_name='manual_wetness', attribute_to_count='value', expected_count=1),
                    # Test only 3 hand hardness categories were used
                    dict(data_name='hand_hardness', attribute_to_count='value', expected_count=3),
                    # Test only 2 graint type categories were used
                    dict(data_name='grain_type', attribute_to_count='value', expected_count=2),
                    # Test only 2 grain_sizes were used
                    dict(data_name='grain_size', attribute_to_count='value', expected_count=2),


                    ]
            }


class TestDensityProfile(TableTestBase):
    '''
    Test that a density file is uploaded correctly including sample
    averaging for the main value.
    '''

    args = ['density.csv']
    kwargs = {'timezone':'MST'}
    UploaderClass = UploadProfileData
    TableClass = LayerData
    dt = datetime.datetime(2020, 2, 5, 13, 30, 0, 0, pytz.timezone('MST'))

    params = {
    'test_count':[dict(data_name='density', expected_count=4)],

    # Test a value from the profile to check that the profile is there and it has integrity
    'test_value': [dict(data_name='density', attribute_to_check='value', filter_attribute='depth', filter_value=35, expected=np.mean([190, 245])),
                   dict(data_name='density', attribute_to_check='sample_a', filter_attribute='depth', filter_value=35, expected=190),
                   dict(data_name='density', attribute_to_check='sample_b', filter_attribute='depth', filter_value=35, expected=245),
                   dict(data_name='density', attribute_to_check='sample_c', filter_attribute='depth', filter_value=35, expected=np.nan),
                   ],
    'test_unique_count': [
                    # Place holder for this test: test only one site_id was added
                    dict(data_name='density', attribute_to_count='site_id', expected_count=1)
                    ]
            }

    def test_bottom_depth(self):
        '''
        Insure bottom depth info is not lost after standardizing it
        '''
        records = self.session.query(LayerData.bottom_depth).filter(LayerData.id <= 2).all()
        assert records[0][0] - records[1][0] == 10


class TestLWCProfile(TableTestBase):
    '''
    Test the permittivity file is uploaded correctly
    '''

    args = ['LWC.csv']
    kwargs = {'timezone':'MST'}
    UploaderClass = UploadProfileData
    TableClass = LayerData
    dt = datetime.datetime(2020, 2, 5, 13, 30, 0, 0, pytz.timezone('MST'))

    params = {
    'test_count':[dict(data_name='permittivity', expected_count=4)],

    # Test a value from the profile to check that the profile is there and it has integrity
    'test_value': [dict(data_name='permittivity', attribute_to_check='value', filter_attribute='depth', filter_value=27, expected=np.mean([1.372, 1.35])),
                   dict(data_name='permittivity', attribute_to_check='sample_a', filter_attribute='depth', filter_value=27, expected=1.372),
                   dict(data_name='permittivity', attribute_to_check='sample_b', filter_attribute='depth', filter_value=27, expected=1.35),
                   dict(data_name='permittivity', attribute_to_check='sample_c', filter_attribute='depth', filter_value=27, expected=None),
                   ],
    'test_unique_count': [
                    # Place holder for this test: test only one location was added
                    dict(data_name='permittivity', attribute_to_count='northing', expected_count=1)
                    ]
            }

class TestLWCProfileB(TableTestBase):
    '''
    Test the permittivity file is uploaded correctly
    '''

    args = ['LWC2.csv']
    kwargs = {'timezone':'MST'}
    UploaderClass = UploadProfileData
    TableClass = LayerData
    dt = datetime.datetime(2020, 3, 12, 14, 45, 0, 0, pytz.timezone('MST'))

    params = {
    'test_count':[dict(data_name='permittivity', expected_count=8)],

    # Test a value from the profile to check that the profile is there and it has integrity
    'test_value': [dict(data_name='permittivity', attribute_to_check='value', filter_attribute='depth', filter_value=73, expected=np.mean([1.507, 1.521])),
                   dict(data_name='permittivity', attribute_to_check='sample_a', filter_attribute='depth', filter_value=73, expected=1.507),
                   dict(data_name='permittivity', attribute_to_check='sample_b', filter_attribute='depth', filter_value=73, expected=1.521),
                   dict(data_name='permittivity', attribute_to_check='sample_c', filter_attribute='depth', filter_value=73, expected=None),
                   # Check lwc_vol
                   dict(data_name='lwc_vol', attribute_to_check='value', filter_attribute='depth', filter_value=15, expected=np.mean([0.1, 0.0])),
                   dict(data_name='lwc_vol', attribute_to_check='sample_a', filter_attribute='depth', filter_value=15, expected=0.1),
                   dict(data_name='lwc_vol', attribute_to_check='sample_b', filter_attribute='depth', filter_value=15, expected=0.0),
                   dict(data_name='lwc_vol', attribute_to_check='sample_c', filter_attribute='depth', filter_value=15, expected=None),
                   # Density
                   dict(data_name='density', attribute_to_check='value', filter_attribute='depth', filter_value=83, expected=164.5),
                   ],

    'test_unique_count': [
                    # Place holder for this test: test only one location was added
                    dict(data_name='permittivity', attribute_to_count='northing', expected_count=1)
                    ]
            }


class TestTemperatureProfile(TableTestBase):
    '''
    Test that a temperature profile is uploaded to the DB correctly
    '''

    args = ['temperature.csv']
    kwargs = {'timezone':'MST'}
    UploaderClass = UploadProfileData
    TableClass = LayerData
    dt = datetime.datetime(2020, 2, 5, 13, 40, 0, 0, pytz.timezone('MST'))

    params = {
    'test_count':[dict(data_name='temperature', expected_count=5)],

    # Test a value from each profile to check that the profile is there and it has integrity
    'test_value': [dict(data_name='temperature', attribute_to_check='value', filter_attribute='depth', filter_value=10, expected=-5.9),
                   dict(data_name='temperature', attribute_to_check='sample_a', filter_attribute='depth', filter_value=35, expected=None),
                   ],
    'test_unique_count': [
                    # Place holder for this test: test only one location was added
                    dict(data_name='temperature', attribute_to_count='northing', expected_count=1)
                    ]
            }

class TestSSAProfile(TableTestBase):
    '''
    Test that all profiles from an SSA file are uploaded correctly
    '''

    args = ['SSA.csv']
    kwargs = {'timezone':'MST'}
    UploaderClass = UploadProfileData
    TableClass = LayerData
    dt = datetime.datetime(2020, 2, 5, 13, 40, 0, 0, pytz.timezone('MST'))

    params = {
    'test_count':[dict(data_name='reflectance', expected_count=16)],

    # Test a value from each profile to check that the profile is there and it has integrity
    'test_value': [dict(data_name='reflectance', attribute_to_check='value', filter_attribute='depth', filter_value=10, expected=22.12),
                   dict(data_name='specific_surface_area', attribute_to_check='value', filter_attribute='depth', filter_value=35, expected=11.2),
                   dict(data_name='equivalent_diameter', attribute_to_check='value', filter_attribute='depth', filter_value=80, expected=0.1054),
                   dict(data_name='sample_signal', attribute_to_check='value', filter_attribute='depth', filter_value=10, expected=186.9),
                   dict(data_name='reflectance', attribute_to_check='comments', filter_attribute='depth', filter_value=5, expected='brush'),

                   ],
    'test_unique_count': [
                    # Confirm we only have 1 comment and everything else is none
                    dict(data_name='reflectance', attribute_to_count='comments', expected_count=2),

                    ]
            }


class TestSMPProfile(TableTestBase):
    '''
    Test SMP profile is uploaded with all its attributes and valid data
    '''

    args = ['S06M0874_2N12_20200131.CSV']
    kwargs = {'timezone':'UTC','units':'Newtons', 'header_sep': ':'}
    UploaderClass = UploadProfileData
    TableClass = LayerData
    dt = datetime.datetime(2020, 1, 31, 22, 42, 14, 0, pytz.timezone('UTC'))

    params = {
    'test_count':[dict(data_name='force', expected_count=154)],
    'test_value': [
                   dict(data_name='force', attribute_to_check='value', filter_attribute='depth', filter_value=-53.17, expected=0.331),
                   dict(data_name='force', attribute_to_check='date', filter_attribute='depth', filter_value=-0.4, expected=dt.date()),
                   dict(data_name='force', attribute_to_check='time', filter_attribute='depth', filter_value=-0.4, expected=dt.timetz()),
                   dict(data_name='force', attribute_to_check='latitude', filter_attribute='depth', filter_value=-0.4, expected=39.03013229370117),
                   dict(data_name='force', attribute_to_check='longitude', filter_attribute='depth', filter_value=-0.4, expected=-108.16268920898438),
                   ],
    'test_unique_count': [dict(data_name='force', attribute_to_count='date', expected_count=1)]
            }

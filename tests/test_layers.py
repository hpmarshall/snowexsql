from sqlalchemy import MetaData
import datetime

from os import remove
from os.path import join, dirname

from snowxsql.create_db import *
from snowxsql.upload import *
from  .sql_test_base import DBSetup

class TestLayers(DBSetup):

    def setup_class(self):
        '''
        Setup the database one time for testing
        '''
        super().setup_class()

        site_fname = join(self.data_dir,'site_details.csv' )
        self.pit = PitHeader(site_fname, 'MST')
        self.bulk_q = \
        self.session.query(LayerData).filter(LayerData.site_id == '1N20')

    def _get_profile_query(self, value_type, depth=None):
        '''
        Construct the query and return it
        '''
        q = self.bulk_q.filter(LayerData.type == value_type)

        if depth != None:
            q = q.filter(LayerData.depth == depth)
        return q

    def get_profile(self, value_type, depth=None):
        '''
        DRYs out the tests for profile uploading

        Args:
            csv: str to path of a csv in the snowex format
            value_type: Type of profile were accessing
        Returns:
            records: List of Layer objects mapped to the database
        '''
        q = self._get_profile_query(value_type, depth=depth)
        records = q.all()
        return records


    def assert_upload(self, csv_f, value_type, n_values):
        '''
        Test whether the correct number of values were uploaded
        '''
        f = join(self.data_dir, csv_f)
        profile = UploadProfileData(f, 'MST', 26912)
        profile.submit(self.session, self.pit.info)

        records = self.get_profile(value_type)
        print(records)
        # Assert N values in the single profile
        assert len(records) == n_values

    def assert_value_assignment(self, value_type, depth, correct_value):
        '''
        Tes whether the correct number of values were uploaded
        '''
        records = self.get_profile(value_type, depth=depth)
        print(records)
        # Assert 5 layers in the single hand hardness profile
        assert getattr(records[0], 'value') == correct_value


class TestStratigraphyProfile(TestLayers):
    '''
    Tests all stratigraphy uploading and value assigning
    '''

    def test_upload(self):
        '''
        Test uploading a stratigraphy csv to the db
        '''
        records = self.assert_upload('stratigraphy.csv','hand_hardness', 5)


    def test_hand_hardness(self):
        '''
        Test uploading a stratigraphy csv to the db
        '''
        self.assert_value_assignment('hand_hardness', 30, '4F')

    def test_grain_size(self):
        '''
        Test uploading a stratigraphy csv to the db
        '''
        self.assert_value_assignment('grain_size', 35, '< 1 mm')

    def test_grain_type(self):
        '''
        Test grain type was assigned
        '''
        self.assert_value_assignment('grain_type', 17, 'FC')

    def test_manual_dampness(self):
        '''
        Test manual dampness was assigned
        '''
        self.assert_value_assignment('manual_dampness', 17, 'D')

    def test_comments_search(self):
        '''
        Testing a specific comment contains query, value confirmation
        '''
        # Check for cups comment assigned to each profile in a stratigraphy file
        q = self.session.query(LayerData)
        records = q.filter(LayerData.comments.contains('Cups')).all()

        # Should be 1 layer for each grain zise, type, hardness, and wetness
        assert len(records) == 4


class TestDensityProfile(TestLayers):

    def test_upload(self):
        '''
        Test uploading a density csv to the db
        '''
        records = self.assert_upload('density.csv','density', 4)

    def test_density_average_assignment(self):
        '''
        Test whether the value of single layer is the average of the samples
        '''
        # Expecting the average of the two density samples
        expected = str((190.0 + 245.0)/2)

        self.assert_value_assignment('density', 35, expected)

    def test_sample_a(self):
        '''
        Tests sample_a value assignment which is renamed from density_a
        '''
        records = self.get_profile('density', depth=25)

        assert getattr(records[0], 'sample_a') == '228.0'

class TestLWCProfile(TestLayers):

    def test_upload(self):
        '''
        Test uploading a lwc csv to the db
        '''
        records = self.get_profile('LWC.csv','dielectric_constant')

        # Check for 4 LWC samples
        assert(len(records)) == 4


    def test_temperature_upload(self):
        '''
        Test uploading a temperature csv to the db
        '''
        records = self.get_profile('temperature.csv','temperature')

        # Assert 5 measurements in the temperature profile
        assert(len(records)) == 5


    def test_ssa_upload(self):
        '''
        Test uploading a SSA csv to the db
        '''
        records = self.get_profile('SSA.csv','specific_surface_area')

        # Check for 16 samples
        assert len(records) == 16

    def test_ssa_value_assignment(self):
        '''
        Test uploading a SSA csv to the db
        '''
        records = self.get_profile('SSA.csv','specific_surface_area', depth=60)

        assert records[0].value == '27.8'

    def test_datatypes(self):
        '''
        Test that all layer attributes in the db are the correct type.
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
        'utm_zone': str,
        'elevation': float,
        'type': str,
        'value': str,
        'depth': float,
        'bottom_depth': float,
        'site_id': str,
        'pit_id': str,
        'slope_angle': int,
        'aspect': int,
        'air_temp': float,
        'total_depth': float,
        'surveyors': str,
        'weather_description': str,
        'precip': str,
        'sky_cover': str,
        'wind': str,
        'ground_condition': str,
        'ground_roughness': str,
        'ground_vegetation': str,
        'vegetation_height': str,
        'tree_canopy': str,
        'site_notes': str,
        'sample_a': str,
        'sample_b': str,
        'sample_c': str,
        'comments': str}

        records = self.bulk_q.all()

        for r in records:
            for c, dtype in dtypes.items():
                db_type = type(getattr(r, c))
                print(c)
                assert (db_type == dtype) or (db_type == type(None))

    def test_geopandas_compliance(self):
        '''
        Test the geometry column exists
        '''
        records = self.session.query(LayerData.geom).limit(1).all()

        # To be compliant with Geopandas must be geom not geometry!
        assert hasattr(records[0], 'geom')

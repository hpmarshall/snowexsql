from os.path import join

import pytest
from sqlalchemy import Table

from snowexsql.db import *
from snowexsql.tables import ImageData, LayerData, PointData, SiteData
from .sql_test_base import DBSetup


class TestDB(DBSetup):
    base_atts = ['site_name', 'date', 'site_id']
    single_loc_atts = ['latitude', 'longitude', 'easting', 'elevation', 'utm_zone', 'geom', 'time']

    meas_atts = ['type', 'units']

    site_atts = base_atts + single_loc_atts + \
                ['slope_angle', 'aspect', 'air_temp', 'total_depth',
                 'weather_description', 'precip', 'sky_cover', 'wind',
                 'ground_condition', 'ground_roughness',
                 'ground_vegetation', 'vegetation_height',
                 'tree_canopy', 'site_notes']

    point_atts = single_loc_atts + meas_atts + \
                 ['version_number', 'equipment', 'value', 'instrument_id']

    layer_atts = single_loc_atts + meas_atts + \
                 ['depth', 'value', 'bottom_depth', 'comments', 'sample_a', 'sample_b',
                  'sample_c']
    raster_atts = meas_atts + ['raster', 'description']

    def setup_class(self):
        """
        Setup the database one time for testing
        """
        super().setup_class()
        site_fname = join(self.data_dir, 'site_details.csv')
        # only reflect the tables we will use
        self.metadata.reflect(self.engine, only=['points', 'layers'])

    def test_point_structure(self):
        """
        Tests our tables are in the database
        """
        t = Table("points", self.metadata, autoload=True)
        columns = [m.key for m in t.columns]

        for c in self.point_atts:
            assert c in columns

    def test_layer_structure(self):
        """
        Tests our tables are in the database
        """
        t = Table("layers", self.metadata, autoload=True)
        columns = [m.key for m in t.columns]

        for c in self.layer_atts:
            assert c in columns

    @pytest.mark.parametrize("DataCls,attributes", [
        (SiteData, site_atts),
        (PointData, point_atts),
        (LayerData, layer_atts),
        (ImageData, raster_atts)])
    def test_get_table_attributes(self, DataCls, attributes):
        """
        Test we return a correct list of table columns from db.py
        """
        atts = get_table_attributes(DataCls)

        for c in attributes:
            assert c in atts


# Independent Tests
@pytest.mark.parametrize("return_metadata, expected_objs", [
    (False, 2),
    (True, 3)])
def test_getting_db(return_metadata, expected_objs):
    """
    Test we can receive a connection and opt out of getting the metadata
    """

    result = get_db('builder:db_builder@localhost/test', return_metadata=return_metadata)
    assert len(result) == expected_objs

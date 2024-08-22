from os.path import join, dirname
import geopandas as gpd
import numpy as np
import pytest
from datetime import date, time
from geoalchemy2.elements import WKTElement
from snowexsql.api import (
    PointMeasurements, LargeQueryCheckException, LayerMeasurements, db_session
)
from snowexsql.db import get_db, initialize
from snowexsql.tables import Instrument, PointData


@pytest.fixture(scope="session")
def data_dir():
    return join(dirname(__file__), 'data')


@pytest.fixture(scope="session")
def creds(data_dir):
    return join(dirname(__file__), 'credentials.json')


@pytest.fixture(scope="session")
def db_url():
    return 'localhost/test'


class DBConnection:
    """
    Base class for connecting to the test database and overwiting the URL
    so that we stay connected to our local testing DB
    """
    CLZ = PointMeasurements

    @pytest.fixture(scope="class")
    def db(self, creds, db_url):
        engine, session, metadata = get_db(
            db_url, credentials=creds, return_metadata=True)

        initialize(engine)
        yield engine
        # cleanup
        session.flush()
        session.rollback()
        metadata.drop_all(bind=engine)
        session.close()

    @staticmethod
    def _add_entry(url, instrument_name, **kwargs):
        url_long = f"{url.username}:{url.password}@{url.host}/{url.database}"
        with db_session(url_long) as (session, engine):
            # Check if the instrument already exists
            instrument = session.query(Instrument).filter_by(
                name=instrument_name).first()

            if not instrument:
                # If the instrument does not exist, create it
                instrument = Instrument(name=instrument_name)
                session.add(instrument)
                session.commit()  # Commit to ensure instrument is saved and has an ID

            # Now that the instrument exists, create the entry, notice we only need the instrument object
            new_entry = PointData(
                instrument=instrument, **kwargs
            )
            session.add(new_entry)
            session.commit()

    @pytest.fixture(scope="class")
    def populated_points(self, db):

        # Fake data to implement
        row = {
            'date': date(2020, 1, 28),
            'time': time(18, 48), 'longitude': -108.13515,
            'latitude': 39.03045,
            'easting': 747987.6190615438, 'northing': 4324061.7062127385,
            'elevation': 3148.2,
            'equipment': 'CRREL_B', 'version_number': 1, 'utm_zone': 12,
            'geom': WKTElement("POINT(747987.6190615438 4324061.7062127385)",
                               srid=26912),
            'site_name': 'Grand Mesa', 'date_accessed': date(2024, 7, 10),
            'value': 94, 'type': 'depth', 'units': 'cm'
        }
        self._add_entry(db.url, 'magnaprobe', **row)

    @pytest.fixture(scope="class")
    def clz(self, db, db_url, populated_points):
        """
        Extend the class and overwrite the database name
        """
        url = db.url
        class Extended(self.CLZ):
            DB_NAME = f"{url.username}:{url.password}@{url.host}/{url.database}"

        yield Extended


def unsorted_list_compare(l1, l2):
    # turn lists into sets, but get rid of any Nones
    l1 = set(l1)
    l2 = set(l2)
    # compare the sets
    return l1 == l2


class TestPointMeasurements(DBConnection):
    """
    Test the Point Measurement class
    """
    CLZ = PointMeasurements

    def test_all_types(self, clz):
        result = clz().all_types
        assert unsorted_list_compare(
            result,
            ['depth']
        )

    def test_all_site_names(self, clz):
        result = clz().all_site_names
        assert unsorted_list_compare(
            result, ['Grand Mesa']
        )

    def test_all_dates(self, clz):
        result = clz().all_dates
        assert len(result) == 1

    def test_all_instruments(self, clz):
        result = clz().all_instruments
        assert unsorted_list_compare(
            result, ["magnaprobe"]
        )

    @pytest.mark.parametrize(
        "kwargs, expected_length, mean_value", [
            ({
                "date": date(2020, 5, 28),
                "instrument": 'camera'
            }, 0, np.nan),
            ({
                 "instrument": "magnaprobe", "limit": 10
             }, 1, 94.0),  # limit works
            ({
                 "date": date(2020, 5, 28),
                 "instrument": 'pit ruler'
             }, 0, np.nan),
            ({
                 "date_less_equal": date(2019, 10, 1),
             }, 0, np.nan),
            ({
                 "date_greater_equal": date(2020, 6, 7),
             }, 0, np.nan),
        ]
    )
    def test_from_filter(self, clz, kwargs, expected_length, mean_value):
        result = clz.from_filter(**kwargs)
        assert len(result) == expected_length
        if expected_length > 0:
            assert pytest.approx(result["value"].mean()) == mean_value

    @pytest.mark.parametrize(
        "kwargs, expected_error", [
            ({"notakey": "value"}, ValueError),
            # ({"instrument": "magnaprobe"}, LargeQueryCheckException),
            ({"date": [date(2020, 5, 28), date(2019, 10, 3)]}, ValueError),
        ]
    )
    def test_from_filter_fails(self, clz, kwargs, expected_error):
        """
        Test failure on not-allowed key and too many returns
        """
        with pytest.raises(expected_error):
            clz.from_filter(**kwargs)

    def test_from_area(self, clz):
        shp = gpd.points_from_xy(
            [743766.4794971556], [4321444.154620216], crs="epsg:26912"
        ).buffer(10)[0]
        result = clz.from_area(
            shp=shp,
            date=date(2019, 10, 30)
        )
        assert len(result) == 0

    def test_from_area_point(self, clz):
        pts = gpd.points_from_xy([743766.4794971556], [4321444.154620216])
        crs = "26912"
        result = clz.from_area(
            pt=pts[0], buffer=10, crs=crs,
            date=date(2019, 10, 30)
        )
        assert len(result) == 0


class TestLayerMeasurements(DBConnection):
    """
    Test the Layer Measurement class
    """
    CLZ = LayerMeasurements

    def test_all_types(self, clz):
        result = clz().all_types
        assert result == []

    def test_all_site_names(self, clz):
        result = clz().all_site_names
        assert result == []

    def test_all_dates(self, clz):
        result = clz().all_dates
        assert len(result) == 0

    def test_all_observers(self, clz):
        result = clz().all_observers
        assert unsorted_list_compare(result, [])

    def test_all_instruments(self, clz):
        result = clz().all_instruments
        assert unsorted_list_compare(result, [])

    @pytest.mark.parametrize(
        "kwargs, expected_length, mean_value", [
            ({
                "date": date(2020, 3, 12), "type": "density",
                "pit_id": "COERIB_20200312_0938"
            }, 0, np.nan),  # filter to 1 pit
            ({"instrument": "IRIS", "limit": 10}, 0, np.nan),  # limit works
            ({
                 "date": date(2020, 5, 28),
                 "instrument": 'IRIS'
             }, 0, np.nan),  # nothing returned
            ({
                "date_less_equal": date(2019, 12, 15),
                "type": 'density'
            }, 0, np.nan),
            ({
                "date_greater_equal": date(2020, 5, 13),
                "type": 'density'
            }, 0, np.nan),
        ]
    )
    def test_from_filter(self, clz, kwargs, expected_length, mean_value):
        result = clz.from_filter(**kwargs)
        assert len(result) == expected_length
        if expected_length > 0:
            assert pytest.approx(
                result["value"].astype("float").mean()
            ) == mean_value

    @pytest.mark.parametrize(
        "kwargs, expected_error", [
            ({"notakey": "value"}, ValueError),
            # ({"date": date(2020, 3, 12)}, LargeQueryCheckException),
            ({"date": [date(2020, 5, 28), date(2019, 10, 3)]}, ValueError),
        ]
    )
    def test_from_filter_fails(self, clz, kwargs, expected_error):
        """
        Test failure on not-allowed key and too many returns
        """
        with pytest.raises(expected_error):
            clz.from_filter(**kwargs)

    def test_from_area(self, clz):
        df = gpd.GeoDataFrame(
            geometry=gpd.points_from_xy(
                [743766.4794971556], [4321444.154620216], crs="epsg:26912"
            ).buffer(1000.0)
        ).set_crs("epsg:26912")
        result = clz.from_area(
            type="density",
            shp=df.iloc[0].geometry,
        )
        assert len(result) == 0

    def test_from_area_point(self, clz):
        pts = gpd.points_from_xy([743766.4794971556], [4321444.154620216])
        crs = "26912"
        result = clz.from_area(
            pt=pts[0], buffer=1000, crs=crs,
            type="density",
        )
        assert len(result) == 0

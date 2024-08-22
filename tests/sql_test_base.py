from os.path import dirname, join

from numpy.testing import assert_almost_equal
from sqlalchemy import asc, text

from snowexsql.db import get_db, initialize


def pytest_generate_tests(metafunc):
    """
    Function used to parametrize functions. If the function is in the
    params keys then run it. Otherwise run all the tests normally.
    """
    # Were params provided?
    if hasattr(metafunc.cls, 'params'):
        if metafunc.function.__name__ in metafunc.cls.params.keys():
            funcarglist = metafunc.cls.params[metafunc.function.__name__]
            argnames = sorted(funcarglist[0])
            metafunc.parametrize(
                argnames, [[funcargs[name] for name in argnames] for funcargs in funcarglist]
            )


class DBSetup:
    """
    Base class for all our tests. Ensures that we clean up after every class that's run
    """

    @classmethod
    def setup_class(self):
        """
        Setup the database one time for testing
        """
        self.db = 'localhost/test'
        self.data_dir = join(dirname(__file__), 'data')
        creds = join(dirname(__file__), 'credentials.json')

        self.engine, self.session, self.metadata = get_db(self.db, credentials=creds, return_metadata=True)

        initialize(self.engine)

    @classmethod
    def teardown_class(self):
        """
        Close the session
        """
        self.session.close()  # optional, depends on use case

    def teardown(self):
        self.session.flush()
        self.session.rollback()


class TableTestBase(DBSetup):
    """
    Test any table by picking
    """
    # Class to use to upload the data
    UploaderClass = None

    # Positional arguments to pass to the uploader class
    args = []

    # Keyword args to pass to the uploader class
    kwargs = {}

    # Always define this using a table class from data.py and is used for ORM
    TableClass = None

    # First filter to be applied is count_attribute == data_name
    count_attribute = 'type'

    # Define params which is a dictionary of test names and their args
    params = {
        'test_count': [dict(data_name=None, expected_count=None)],
        'test_value': [
            dict(data_name=None, attribute_to_check=None, filter_attribute=None, filter_value=None, expected=None)],
        'test_unique_count': [dict(data_name=None, attribute_to_count=None, expected_count=None)]
    }

    @classmethod
    def setup_class(self):
        """
        Setup the database one time for testing
        """
        super().setup_class()

        # Batches always provide a list of files
        if type(self.args[0]) == list:
            self.args[0] = [join(self.data_dir, f) for f in self.args[0]]
        # Single uploaders only upload a single file
        else:
            self.args[0] = join(self.data_dir, self.args[0])

        # In case we have a smp_log file make it point to the data folder too
        if 'smp_log_f' in self.kwargs.keys():
            if self.kwargs['smp_log_f'] != None:
                self.kwargs['smp_log_f'] = join(self.data_dir, self.kwargs['smp_log_f'])

        self.kwargs['db_name'] = self.db
        self.kwargs['credentials'] = join(dirname(__file__), 'credentials.json')
        u = self.UploaderClass(*self.args, **self.kwargs)

        # Allow for batches and single upload
        if 'batch' in self.UploaderClass.__name__.lower():
            u.push()
        else:
            u.submit(self.session)

    def get_query(self, filter_attribute, filter_value, query=None):
        """
        Return the base query using an attribute and value that it is supposed
        to be

        Args:
            filter_attribute: Name of attribute to search for
            filter_value: Value that attribute should be to filter db search
            query: If were extended a query use it instead of forming a new one
        Return:
            q: Uncompiled SQLalchemy Query object
        """

        if query is None:
            query = self.session.query(self.TableClass)

        fa = getattr(self.TableClass, filter_attribute)
        q = query.filter(fa == filter_value).order_by(asc(fa))
        return q

    def test_count(self, data_name, expected_count):
        """
        Test the record count of a data type
        """
        q = self.get_query(self.count_attribute, data_name)
        records = q.all()
        assert len(records) == expected_count

    def test_value(self, data_name, attribute_to_check, filter_attribute, filter_value, expected):
        """
        Test that the first value in a filtered record search is as expected
        """
        # Filter  to the data type were querying
        q = self.get_query(self.count_attribute, data_name)

        # Add another filter by some attribute
        q = self.get_query(filter_attribute, filter_value, query=q)

        records = q.all()
        received = getattr(records[0], attribute_to_check)

        try:
            received = float(received)
        except:
            pass

        if type(received) == float:
            assert_almost_equal(received, expected, 6)
        else:
            assert received == expected

    def test_unique_count(self, data_name, attribute_to_count, expected_count):
        """
        Test that the number of unique values in a given attribute is as expected
        """
        # Add another filter by some attribute
        q = self.get_query(self.count_attribute, data_name)
        records = q.all()
        received = len(set([getattr(r, attribute_to_count) for r in records]))
        assert received == expected_count

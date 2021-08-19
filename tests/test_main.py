import unittest
import time
from unittest import mock
from neon.functions import *

LOGGER = logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)
PATH = os.path.abspath(os.path.dirname(__file__))

class TestAPI(unittest.TestCase):
    """
    All tests for the API itself go here.
    """
    @classmethod
    def setUpClass(cls) -> None:
        cls.logging = logging.getLogger("TestSession")
        cls.logging.debug("Starting test session...")
        cls.mocked_data : List[dict] = [{
            "fact" : "This is a random fact",
            "length" : "-99"
        },{
            "fact" : "This is a fun fact",
            "length" : "-98"
        },{
            "fact" : "This is a sad fact",
            "length" : "-97"
        }]

    def test_retrieve_data_with_waiting(self):
        actual: dict = retrieve_data(waiting=5)
        actual_keys = [key for key in actual]
        expected_keys = ["fact", "length"]
        self.assertEqual(actual_keys, expected_keys)

    @mock.patch("neon.functions.make_request", return_value=None)
    def test_retrieve_data_without_waiting(self, patched_request):
        actual: dict = retrieve_data(waiting=5)
        actual_keys = [key for key in actual]
        expected_keys = ["fact", "length"]
        self.assertEqual(actual_keys, expected_keys)
    
    @mock.patch("neon.functions.retrieve_data")
    def test_process_data_with_random_load(self, patched_retrieve):

        actual = process_data(usernumber=5, waiting=1)
        pass

    @mock.patch("neon.functions.make_request", return_value=None)
    def test_process_data_with_api_load(self, patched_request):
        pass


class TestSparkFunctions(unittest.TestCase):
    """
    All tests for Spark-related functions go here.
    """
    @mock.patch("pyspark.sql.readwriter.DataFrameWriter.saveAsTable")
    def test_group_and_save_with_random_load(self, patched_writer):
        pass

    @mock.patch("pyspark.sql.readwriter.DataFrameWriter.saveAsTable")
    def test_group_and_save_with_api_load(self, patched_writer):
        pass

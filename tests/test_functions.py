import unittest
import requests
import time

from requests.exceptions import ConnectionError
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
        cls.mocked_data: List[dict] = [{
            "fact": "This is a random fact",
            "length": "-99"
        }, {
            "fact": "This is a fun fact",
            "length": "-98"
        }, {
            "fact": "This is a sad fact",
            "length": "-97"
        }]
        cls.requests = cls.establishOnlineStatus()
    
    # TODO: this should define whether the system is online or not.
    # If not online, could be cool to use a patched version of requests.
    @classmethod
    def establishOnlineStatus(cls):
        try:
            requests.get("https://catfact.ninja/fact")
            return requests
        except ConnectionError:
            patched_requests = mock.create_autospec(requests)
            patched_requests.get.json.return_value = cls.mocked_data
            return patched_requests

    # FIXME: mock has no access to class methods, how to do that?
    #@mock.patch("neon.functions.requests", new=self.requests)
    def test_retrieve_data_with_waiting(self):
        t0 = time.perf_counter()
        actual: dict = retrieve_data(waiting=5)
        actual_keys: list = [key for key in actual]
        expected_keys: list = ["fact", "length"]
        t1 = time.perf_counter() - t0
        self.assertEqual(actual_keys, expected_keys)
        self.assertAlmostEqual(t1, 5, delta=1)

    @mock.patch("neon.functions.make_request", return_value=None)
    def test_retrieve_data_without_waiting(self, patched_request):
        t0 = time.perf_counter()
        actual: dict = retrieve_data(waiting=5)
        actual_keys: list = [key for key in actual]
        expected_keys: list = ["fact", "length"]
        t1 = time.perf_counter() - t0
        self.assertEqual(actual_keys, expected_keys)
        self.assertLess(t1, 5)

    @mock.patch("neon.functions.retrieve_data", return_value={
        "fact": "This is a random fact",
        "length": "-99"
    })
    def test_process_data_with_custom_load(self, patched_retrieve):
        t0 = time.perf_counter()
        actual: list = process_data(usernumber=5, waiting=2)
        expected: list = [{"fact": "This is a random fact", "length": "-99"}]*5
        t1 = time.perf_counter() - t0
        self.assertEqual(actual, expected)
        self.assertLess(t1, 5)


class TestSparkFunctions(unittest.TestCase):
    """
    All tests for Spark-related functions go here.
    """
    @classmethod
    def setUpClass(cls) -> None:
        cls.logging = logging.getLogger("TestSession")
        cls.logging.debug("Starting test session...")
        cls.mocked_data: List[dict] = [{
            "fact": "This is a random fact",
            "length": "-99"
        }, {
            "fact": "This is a fun fact",
            "length": "-98"
        }, {
            "fact": "This is a sad fact",
            "length": "-97"
        }]

    def test_group_and_save_with_random_load(self):
        mocked_save = mock.create_autospec(group_and_save)
        spark: SparkSession = establish_spark()
        data: List[dict] = self.mocked_data
        expected: bool = mocked_save(spark, data)
        self.assertTrue(expected)

    @mock.patch("pyspark.sql.readwriter.DataFrameWriter.saveAsTable")
    def test_group_and_save_with_patching(self, patched_writer):
        patched_writer.new = True
        spark: SparkSession = establish_spark()
        data: List[dict] = self.mocked_data
        group_and_save(spark, data)
        patched_writer.assert_called()

    @mock.patch("neon.functions.make_request", return_value=None)
    def test_group_and_save_with_api_load(self, patched_request):
        mocked_save = mock.create_autospec(group_and_save)
        spark: SparkSession = establish_spark()
        data: list[dict] = process_data(usernumber=5, waiting=2)
        expected: bool = mocked_save(spark, data)
        self.assertTrue(expected)

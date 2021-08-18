import unittest


class TestAPI(unittest.TestCase):
    """
    All tests for the API itself go here.
    """
    @classmethod
    def setUpClass(cls) -> None:
        pass

    @classmethod
    def tearDownClass(cls) -> None:
        pass

    def test_retrieve_data_with_waiting(self):
        pass

    def test_retrieve_data_without_waiting(self):
        pass

    def test_process_data_with_random_load(self):
        pass

    def test_process_data_with_api_load(self):
        pass


class TestSparkFunctions(unittest.TestCase):
    """
    All tests for Spark-related functions go here.
    """
    @classmethod
    def setUpClass(cls) -> None:
        pass

    @classmethod
    def tearDownClass(cls) -> None:
        pass

    def test_group_and_save_with_random_load(self):
        pass

    def test_group_and_save_with_api_load(self):
        pass

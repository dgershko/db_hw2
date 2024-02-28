import unittest
import Solution as Solution


class AbstractTest(unittest.TestCase):
    # before each test, setUp is executed
    def setUp(self) -> None:
        Solution.create_tables()

    # after each test, tearDown is executed
    def tearDown(self) -> None:
        Solution.drop_tables()

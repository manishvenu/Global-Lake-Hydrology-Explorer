#  tests/runner.py
import unittest

# import your test modules
import tests.initial_tests

# initialize the test suite
loader = unittest.TestLoader()
suite  = unittest.TestSuite()

# add a specific test to the test suite
suite.addTests(loader.loadTestsFromName('tests.initial_tests.MyTestCase.test_download_data_check'))

# initialize a runner, pass it your suite and run it
runner = unittest.TextTestRunner(verbosity=3)
result = runner.run(suite)
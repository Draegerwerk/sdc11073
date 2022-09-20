import os
import unittest
import tests
import HtmlTestRunner


if __name__ == '__main__':
    loader = unittest.TestLoader()
    all_tests = loader.discover(os.path.dirname(tests.__file__))

    if not all_tests.countTestCases():
        raise RuntimeError("No unittests are found.")

    runner = HtmlTestRunner.HTMLTestRunner(output='unittest_results',
                                           combine_reports=True,
                                           report_name=f"UnittestReport",
                                           report_title=f'sdc11073 unit tests')
    runner.run(all_tests)

import unittest
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from tcdsutils import *

class TestServiceUtils(unittest.TestCase):
    class ServiceUtilsChild(ServiceUtils):
        def __init__(self, resolv):
            ServiceUtils.__init__(self, resolv)
            self._config_file_path_prefix = '/abc/def'

    def test_service_utils_constants(self):
        resolv = LocalResolv()
        utils = self.ServiceUtilsChild(resolv)
        self.assertEquals(utils.SUCCESS, 0)
        self.assertEquals(utils.ERROR_GENERAL, 1)
        self.assertEquals(utils.resolv, resolv)
        self.assertEquals(utils.CONFIG_FILE_PATH_PREFIX, '/abc/def')
        try:
            utils.SUCCESS = 1
        except AttributeError:
            pass
        else:
            self.asserttrue(False)
        try:
            utils.ERROR_GENERAL = 2
        except AttributeError:
            pass
        else:
            self.asserttrue(False)
        try:
            utils.resolv = LocalResolv()
        except AttributeError:
            pass
        else:
            self.asserttrue(False)
        try:
            utils.CONFIG_FILE_PATH_PREFIX = '1'
        except AttributeError:
            pass
        else:
            self.asserttrue(False)


if __name__ == '__main__':
    unittest.main()
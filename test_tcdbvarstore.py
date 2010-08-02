import unittest
import uuid
from test_varstore import TestVarStore
from varstore import *
from service import TcdsService
from depot import Depot
from daemon import Mon
from serviceglobals import TcServiceGlobals as Globals
from serviceglobals import TcdbResolv as Resolv


class TestTcdbVarStore(TestVarStore):
    def setUp(self):
        self.depot_uuid = 'd2c5d946-b888-40b3-aac2-adda05477a81'
        try:
            self.var = TcdbVarStore()
        except Exception as e:
            print e
            self.var = None

    def run(self, *args, **kwds):
        unittest.TestCase.run(self, *args, **kwds)

if __name__ == '__main__':
    unittest.main()

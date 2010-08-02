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
        self.depot_uuid = str(uuid.uuid4())
        exec 'from serviceglobals import Tcdb'
        conn = Tcdb.connect()
        cur = conn.cursor()
        cur.execute('insert into "SERVICE_GROUP" values (%s, 4, 5)', (self.depot_uuid,))
        conn.commit()
        cur.close()
        conn.close()
        try:
            self.var = TcdbVarStore()
        except Exception as e:
            print e
            self.var = None

    def run(self, *args, **kwds):
        unittest.TestCase.run(self, *args, **kwds)

    def tearDown(self):
        exec 'from serviceglobals import Tcdb'
        conn = Tcdb.connect()
        cur = conn.cursor()
        cur.execute('delete from "SERVICE_GROUP" where "ID"=%s', (self.depot_uuid,))
        conn.commit()
        cur.close()
        conn.close()

if __name__ == '__main__':
    unittest.main()

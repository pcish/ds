import unittest
import uuid
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from test_varstore import TestVarStore
from varstore import *


class TestTcdbVarStore(TestVarStore):
    def setUp(self):
        super(TestTcdbVarStore, self).setUp()
        self.var = TcdbVarStore()
        exec 'from tcdsutils import Tcdb'
        conn = Tcdb.connect()
        cur = conn.cursor()
        cur.execute('insert into "SERVICE_GROUP" values (%s, 4, 5)', (self.depot_uuid,))
        conn.commit()
        cur.execute('insert into "SERVICE_GROUP" values (%s, 4, 5)', (self.depot_uuid2,))
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
        exec 'from tcdsutils import Tcdb'
        conn = Tcdb.connect()
        cur = conn.cursor()
        cur.execute('delete from "SERVICE_GROUP" where "ID"=%s', (self.depot_uuid,))
        conn.commit()
        cur.execute('delete from "SERVICE_GROUP" where "ID"=%s', (self.depot_uuid2,))
        conn.commit()
        cur.close()
        conn.close()

if __name__ == '__main__':
    unittest.main()

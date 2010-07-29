import unittest
from depot import Depot
from daemon import Mon, Mds, Osd
from varstore import LocalVarStore
from serviceglobals import LocalDebugServiceGlobals as Globals

class TestDepot(unittest.TestCase):
    def test_min_req_check(self):
        self.assertEquals(Depot._get_meets_min_requirements(3, 3, 2, 3), True)
        self.assertEquals(Depot._get_meets_min_requirements(3, 3, 2, 2), False)
        self.assertEquals(Depot._get_meets_min_requirements(3, 3, 1, 3), False)
        self.assertEquals(Depot._get_meets_min_requirements(2, 2, 3, 3), False)
        self.assertEquals(Depot._get_meets_min_requirements(3, 3, 2, 4), True)
        self.assertEquals(Depot._get_meets_min_requirements(3, **{'num_osd': 3, 'num_mon':4, 'num_mds': 1}), False)
        self.assertRaises(ValueError, Depot._get_meets_min_requirements, 0, 3, 2, 3)
        self.assertRaises(ValueError, Depot._get_meets_min_requirements, 3, -1, 2, 3)
        self.assertRaises(TypeError, Depot._get_meets_min_requirements, 3.5, 3, 2, 3)
        self.assertRaises(TypeError, Depot._get_meets_min_requirements, '3', 3, 2, 3)

class TestAddDaemon(unittest.TestCase):
    depot = None
    def setUp(self):
        self.depot = Depot(service_globals=Globals(), id="...", varstore=LocalVarStore())
        self.depot.activate()

    def test_add_osd(self):
        daemon = Osd(self.depot.service_globals, self.depot.var)
        self.depot._add_daemon(None, daemon)


class Test_check_ceph_ids_are_consequtive(unittest.TestCase):
    depot = None
    def setUp(self):
        varstore = LocalVarStore()
        self.depot = Depot(Globals(), None, varstore)

    def test_check_ceph_ids_are_consequtive(self):
        daemon = Osd(self.depot.service_globals, self.depot.var)
        self.depot._add_daemon(None, daemon)
        self.assertEquals(self.depot._check_ceph_ids_are_consequtive(), True)
        daemon.set_ceph_id(2)
        self.assertEquals(self.depot._check_ceph_ids_are_consequtive(), False)

if __name__ == '__main__':
    unittest.main()

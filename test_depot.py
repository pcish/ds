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

class TestAddDaemonToOnlineDepot(unittest.TestCase):
    depot = None
    def setUp(self):
        self.depot = Depot(service_globals=Globals(), id="...", varstore=LocalVarStore())
        self.depot.activate()

    def test_add_osd(self):
        daemon = Osd(self.depot, 'localhost', 0)
        self.depot._add_daemon(daemon)


class Test_check_ceph_ids_are_consequtive(unittest.TestCase):
    depot = None
    def setUp(self):
        varstore = LocalVarStore()
        self.depot = Depot(Globals(), None, varstore)

    def test_check_ceph_ids_are_consequtive(self):
        daemon = Osd(self.depot, None, 0)
        self.depot._add_daemon(daemon)
        self.assertEquals(self.depot._check_ceph_ids_are_consequtive(), True)
        daemon.set_ceph_id(2)
        self.assertEquals(self.depot._check_ceph_ids_are_consequtive(), False)

class Test_get_next_ceph_name_for(unittest.TestCase):
    depot = None
    def setUp(self):
        self.depot = Depot(Globals(), None, LocalVarStore())

    def test_get_next_ceph_name_for(self):
        self.assertEquals(self.depot._get_next_ceph_name_for('mon'), 0)
        self.assertEquals(self.depot._get_next_ceph_name_for('mds'), 0)
        self.assertEquals(self.depot._get_next_ceph_name_for('osd'), 0)
        daemon = Osd(self.depot, None, 0)
        self.depot._add_daemon(daemon)
        self.assertEquals(self.depot._get_next_ceph_name_for('mon'), 0)
        self.assertEquals(self.depot._get_next_ceph_name_for('mds'), 0)
        self.assertEquals(self.depot._get_next_ceph_name_for('osd'), 1)

if __name__ == '__main__':
    unittest.main()

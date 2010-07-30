import unittest
import uuid
from depot import Depot
from service import TcdsService
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
        service = TcdsService(LocalVarStore(), Globals())
        self.depot = Depot(service, None)
        self.depot.activate()

    def test_add_osd(self):
        node_list = [{'node_id': str(uuid.uuid4()), 'storage_roles': ['osd']}]
        self.depot.add_nodes(node_list)


class Test_check_ceph_ids_are_consecutive(unittest.TestCase):
    depot = None
    def setUp(self):
        service = TcdsService(LocalVarStore(), Globals())
        self.depot = Depot(service, None)

    def test_check_ceph_ids_are_consecutive(self):
        node_uuid = str(uuid.uuid4())
        node_list = [{'node_id': node_uuid, 'storage_roles': ['osd']}]
        self.depot.add_nodes(node_list)
        self.assertEquals(self.depot._check_ceph_ids_are_consecutive(), True)
        daemon_list = self.depot.get_daemon_list()
        for daemon in daemon_list:
            if daemon.get_
        daemon.set_ceph_id(2)
        self.assertEquals(self.depot._check_ceph_ids_are_consecutive(), False)

class Test_get_next_ceph_name_for(unittest.TestCase):
    depot = None
    def setUp(self):
        service = TcdsService(LocalVarStore(), Globals())
        self.depot = Depot(service, None)

    def test_get_next_ceph_name_for(self):
        self.assertEquals(self.depot._get_next_ceph_name_for('mon'), 0)
        self.assertEquals(self.depot._get_next_ceph_name_for('mds'), 0)
        self.assertEquals(self.depot._get_next_ceph_name_for('osd'), 0)
        node_list = [{'node_id': str(uuid.uuid4()), 'storage_roles': ['osd']}]
        self.depot.add_nodes(node_list)
        self.assertEquals(self.depot._get_next_ceph_name_for('mon'), 0)
        self.assertEquals(self.depot._get_next_ceph_name_for('mds'), 0)
        self.assertEquals(self.depot._get_next_ceph_name_for('osd'), 1)

if __name__ == '__main__':
    unittest.main()

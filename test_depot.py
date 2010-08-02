import unittest
import uuid
from depot import Depot
from service import TcdsService
from daemon import Mon, Mds, Osd
from varstore import LocalVarStore
from serviceglobals import LocalUnittestServiceGlobals as Globals
from serviceglobals import LocalResolv as Resolv

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
    service = None
    def setUp(self):
        self.service = TcdsService(Globals(Resolv()), LocalVarStore())
        self.depot = self.service.create_depot('test_depot', 3)
        host = str(uuid.uuid4())
        node_list = [{'uuid': str(uuid.uuid4()), 'type': 'mon', 'host': host}]
        self.depot.add_daemons(node_list)
        self.depot.activate()

    def test_add_osd(self):
        self.service.service_globals.clear_shell_commands()
        node_list = [{'uuid': str(uuid.uuid4()), 'type': 'osd', 'host': str(uuid.uuid4())}]
        self.depot.add_daemons(node_list)
        self.assertTrue('ssh  /etc/init.d/ceph -c test_depot.conf --hostname  start osd' in self.service.service_globals.shell_commands)

    def test_add_mon(self):
        self.service.service_globals.clear_shell_commands()
        node_list = [{'uuid': str(uuid.uuid4()), 'type': 'mon', 'host': str(uuid.uuid4())}]
        self.depot.add_daemons(node_list)
        self.assertTrue('ssh  /etc/init.d/ceph -c test_depot.conf --hostname  start mon' in self.service.service_globals.shell_commands)

    def test_add_mds(self):
        self.service.service_globals.clear_shell_commands()
        node_list = [{'uuid': str(uuid.uuid4()), 'type': 'mds', 'host': str(uuid.uuid4())}]
        self.depot.add_daemons(node_list)
        self.assertTrue('ssh  /etc/init.d/ceph -c test_depot.conf --hostname  start mds' in self.service.service_globals.shell_commands)

class Test_check_ceph_ids_are_consecutive(unittest.TestCase):
    depot = None
    def setUp(self):
        service = TcdsService(Globals(Resolv()), LocalVarStore())
        self.depot = service.create_depot('test_depot', 3)

    def test_check_ceph_ids_are_consecutive(self):
        node_uuid = str(uuid.uuid4())
        node_list = [{'uuid': node_uuid, 'type': 'osd', 'host': ''}]
        self.depot.add_daemons(node_list)
        self.assertEquals(self.depot._check_ceph_ids_are_consecutive(), True)
        daemon_list = self.depot.get_daemon_list()
        for daemon in daemon_list:
            if daemon.get_uuid() == node_uuid:
                daemon.set_ceph_name(1)
                break
        self.assertEquals(self.depot._check_ceph_ids_are_consecutive(), False)
        node_uuid = str(uuid.uuid4())
        node_list = [{'uuid': node_uuid, 'type': 'osd', 'host': ''}]
        self.depot.add_daemons(node_list)
        self.assertEquals(self.depot._check_ceph_ids_are_consecutive(), True)

class Test_get_next_ceph_name_for(unittest.TestCase):
    depot = None
    def setUp(self):
        service = TcdsService(Globals(Resolv()), LocalVarStore())
        self.depot = service.create_depot('test_depot', 3)

    def test_get_next_ceph_name_for(self):
        self.assertEquals(self.depot._get_next_ceph_name_for('mon'), 0)
        self.assertEquals(self.depot._get_next_ceph_name_for('mds'), 0)
        self.assertEquals(self.depot._get_next_ceph_name_for('osd'), 0)
        node_uuid = str(uuid.uuid4())
        node_list = [{'uuid': node_uuid, 'type': 'osd', 'host': ''}]
        self.depot.add_daemons(node_list)
        self.assertEquals(self.depot._get_next_ceph_name_for('mon'), 0)
        self.assertEquals(self.depot._get_next_ceph_name_for('mds'), 0)
        self.assertEquals(self.depot._get_next_ceph_name_for('osd'), 1)
        node_uuid = str(uuid.uuid4())
        node_list = [{'uuid': node_uuid, 'type': 'osd', 'host': ''}]
        (daemon,) = self.depot.add_daemons(node_list)
        daemon.set_ceph_name('!')
        self.assertEquals(self.depot._get_next_ceph_name_for('mon'), 0)
        self.assertEquals(self.depot._get_next_ceph_name_for('mds'), 0)
        self.assertEquals(self.depot._get_next_ceph_name_for('osd'), 1)

if __name__ == '__main__':
    unittest.main()

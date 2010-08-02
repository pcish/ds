import unittest
import uuid
from varstore import *
from service import TcdsService
from depot import Depot
from daemon import Mon, Mds
from serviceglobals import LocalUnittestServiceGlobals as Globals
from serviceglobals import LocalResolv as Resolv

class TestVarStore(unittest.TestCase):
    var = None
    depot_uuid = ''
    def setUp(self):
        self.var = None
        self.depot_uuid = str(uuid.uuid4())

    def run(self, *args, **kwds): pass

    def test_add_del_depot(self):
        service = TcdsService(Globals(Resolv()), self.var)
        uuidstr = self.depot_uuid
        depot = Depot(service, uuidstr)
        self.var.add_depot(depot, uuidstr, 30, depot.CONSTANTS['STATE_OFFLINE'])
        self.assertEquals(self.var.get_depot_state(depot), depot.CONSTANTS['STATE_OFFLINE'])
        self.assertEquals(self.var.get_depot_replication_factor(depot), 30)
        self.var.del_depot(depot)
        self.assertRaises(KeyError, self.var.get_depot_state, depot)

    def test_set_depot_uuid(self):
        service = TcdsService(Globals(Resolv()), self.var)
        uuidstr = self.depot_uuid
        depot = Depot(service, uuidstr)
        self.var.add_depot(depot, uuidstr, 30, depot.CONSTANTS['STATE_OFFLINE'])
        self.assertEquals(self.var.get_depot_state(depot), depot.CONSTANTS['STATE_OFFLINE'])
        new_uuidstr = str(uuid.uuid4())
        self.var.set_depot_uuid(depot, new_uuidstr)
        self.assertRaises(KeyError, self.var.get_depot_state, depot)
        depot.uuid = new_uuidstr
        self.assertEquals(self.var.get_depot_state(depot), depot.CONSTANTS['STATE_OFFLINE'])

    def test_set_depot_state(self):
        service = TcdsService(Globals(Resolv()), self.var)
        uuidstr = self.depot_uuid
        depot = Depot(service, uuidstr)
        self.var.add_depot(depot, uuidstr, 30, depot.CONSTANTS['STATE_OFFLINE'])
        self.assertEquals(self.var.get_depot_state(depot), depot.CONSTANTS['STATE_OFFLINE'])
        self.var.set_depot_state(depot, depot.CONSTANTS['STATE_ONLINE'])
        self.assertEquals(self.var.get_depot_state(depot), depot.CONSTANTS['STATE_ONLINE'])

    def test_set_depot_replication_factor(self):
        service = TcdsService(Globals(Resolv()), self.var)
        uuidstr = self.depot_uuid
        depot = Depot(service, uuidstr)
        self.var.add_depot(depot, uuidstr, 3, depot.CONSTANTS['STATE_OFFLINE'])
        self.assertEquals(self.var.get_depot_replication_factor(depot), 3)
        self.var.set_depot_replication_factor(depot, 26)
        self.assertEquals(self.var.get_depot_replication_factor(depot), 26)

    def test_add_remove_daemon(self):
        service = TcdsService(Globals(Resolv()), self.var)
        uuidstr = self.depot_uuid
        depot = Depot(service, uuidstr)
        self.var.add_depot(depot, uuidstr, 30, depot.CONSTANTS['STATE_OFFLINE'])
        uuidstr = str(uuid.uuid4())
        daemon = Mon(depot, uuidstr)
        host = str(uuid.uuid4())
        ceph_name = 0
        self.var.add_daemon(daemon, uuidstr, host, ceph_name)
        self.assertEquals(self.var.get_daemon_host(daemon), host)
        self.assertEquals(self.var.get_daemon_ceph_name(daemon), str(ceph_name))
        self.var.remove_daemons((daemon,))
        self.assertRaises(KeyError, self.var.get_daemon_ceph_name, daemon)

    def test_get_depot_daemon_list(self):
        service = TcdsService(Globals(Resolv()), self.var)
        uuidstr = self.depot_uuid
        depot = Depot(service, uuidstr)
        self.var.add_depot(depot, uuidstr, 3, depot.CONSTANTS['STATE_OFFLINE'])
        uuidstr = str(uuid.uuid4())
        daemon = Mon(depot, uuidstr)
        host = str(uuid.uuid4())
        ceph_name = 0
        self.var.add_daemon(daemon, uuidstr, host, ceph_name)
        self.assertEquals(self.var.get_depot_daemon_list(depot), [{'type': 'mon', 'host': host, 'ceph_name': '0', 'uuid': uuidstr}])
        uuidstr2 = str(uuid.uuid4())
        daemon2 = Mds(depot, uuidstr2)
        host2 = str(uuid.uuid4())
        ceph_name2 = 'b'
        self.var.add_daemon(daemon2, uuidstr2, host2, ceph_name2)
        self.assertTrue({'type': 'mon', 'host': host, 'ceph_name': '0', 'uuid': uuidstr} in self.var.get_depot_daemon_list(depot))
        self.assertTrue({'type': 'mds', 'host': host2, 'ceph_name': 'b', 'uuid': uuidstr2} in self.var.get_depot_daemon_list(depot))

    """
    def set_daemon_uuid(self, daemon, uuid):
        daemon.depot._localvars['daemons'][uuid] = daemon.depot._localvars['daemons'][daemon.uuid]
        del daemon.depot._localvars['daemons'][daemon.uuid]

    def set_daemon_host(self, daemon, host_uuid):
        daemon._localvars['host'] = host_uuid

    def get_daemon_host(self, daemon):
        return daemon._localvars['host']

    def set_daemon_ceph_name(self, daemon, ceph_name):
        daemon._localvars['ceph_name'] = str(ceph_name)

    def get_daemon_ceph_name(self, daemon):
        return daemon._localvars['ceph_name']
    """

class TestLocalVarStore(TestVarStore):
    def setUp(self):
        self.var = LocalVarStore()
    def run(self, *args, **kwds):
        unittest.TestCase.run(self, *args, **kwds)


if __name__ == '__main__':
    unittest.main()
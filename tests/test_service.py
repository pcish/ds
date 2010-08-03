import unittest
import uuid
import sys, os
sys.path.append(os.path.abspath(os.path.join(__file__, '../..')))
from service import TcdsService
from depot import Depot
from daemon import Mon, Mds, Osd
from varstore import LocalVarStore
from serviceglobals import LocalUnittestServiceUtils
from serviceglobals import LocalResolv

class TestCreateDepot(unittest.TestCase):
    service = None
    def setUp(self):
        resolv = LocalResolv()
        resolv.mapping['0895d363-2972-4c40-9f5b-0df2b224a2c6'] = '10.0.0.1'
        resolv.mapping['a0e6fbf4-e6d2-4a7a-97d3-9390703d6b3a'] = '10.0.0.2'
        resolv.mapping['92144222-e7b6-4c13-aeb8-7a32cd2c6458'] = '10.0.0.3'
        resolv.mapping['f97f46ee-7d40-4385-b9a0-7b46079d699b'] = '10.0.0.4'
        resolv.mapping['c8634dc9-ddc6-41c4-ba12-b1d4b5523e2e'] = '10.0.0.5'
        resolv.mapping['f0797c41-b21f-4eda-8093-32285453d035'] = '10.0.0.6'
        resolv.mapping['f8511822-1520-41a8-8638-6dca4c074b65'] = '10.0.0.7'
        self.service = TcdsService(LocalUnittestServiceUtils(resolv), LocalVarStore())

    def test_create_depot(self):
        depot_id = str(uuid.uuid4())
        replication = 3
        depot = self.service.create_depot(depot_id, replication)
        self.assertEquals(depot.uuid, depot_id)
        self.assertEquals(depot.var.get_depot_replication_factor(depot), replication)

        daemon_spec_list = []
        daemon_spec_list.append({'type': 'mon', 'host': '0895d363-2972-4c40-9f5b-0df2b224a2c6', 'uuid': str(uuid.uuid4())})
        daemon_spec_list.append({'type': 'osd', 'host': '0895d363-2972-4c40-9f5b-0df2b224a2c6', 'uuid': str(uuid.uuid4())})
        daemon_spec_list.append({'type': 'mon', 'host': 'a0e6fbf4-e6d2-4a7a-97d3-9390703d6b3a', 'uuid': str(uuid.uuid4())})
        daemon_spec_list.append({'type': 'osd', 'host': 'a0e6fbf4-e6d2-4a7a-97d3-9390703d6b3a', 'uuid': str(uuid.uuid4())})
        daemon_spec_list.append({'type': 'mon', 'host': '92144222-e7b6-4c13-aeb8-7a32cd2c6458', 'uuid': str(uuid.uuid4())})
        daemon_spec_list.append({'type': 'osd', 'host': '92144222-e7b6-4c13-aeb8-7a32cd2c6458', 'uuid': str(uuid.uuid4())})
        daemon_spec_list.append({'type': 'mds', 'host': 'f97f46ee-7d40-4385-b9a0-7b46079d699b', 'uuid': str(uuid.uuid4())})
        daemon_spec_list.append({'type': 'mds', 'host': 'c8634dc9-ddc6-41c4-ba12-b1d4b5523e2e', 'uuid': str(uuid.uuid4())})
        self.service.add_daemons_to_depot(depot_id, daemon_spec_list)

        daemons = self.service._depot_map[depot_id].get_daemon_list()
        self.assertEquals(len(self.service._depot_map[depot_id].get_daemon_list('mon')), 3)
        self.assertEquals(len(self.service._depot_map[depot_id].get_daemon_list('osd')), 3)
        self.assertEquals(len(self.service._depot_map[depot_id].get_daemon_list('mds')), 2)

        self.service.utils.clear_shell_commands()
        daemon_spec_list_m = []
        daemon_spec_list_m.append({'type': 'mon', 'host': 'f8511822-1520-41a8-8638-6dca4c074b65', 'uuid': str(uuid.uuid4())})
        self.service.add_daemons_to_depot(depot_id, daemon_spec_list_m)
        print self.service.utils.shell_commands
        self.assertTrue('ceph -c %s.conf mon add 3 10.0.0.7:6789' % depot_id in self.service.utils.shell_commands)

        self.service.utils.clear_shell_commands()
        daemon_spec_list_o = []
        daemon_spec_list_o.append({'type': 'osd', 'host': 'f0797c41-b21f-4eda-8093-32285453d035', 'uuid': str(uuid.uuid4())})
        self.service.add_daemons_to_depot(depot_id, daemon_spec_list_o)
        print self.service.utils.shell_commands

        node_list = []
        for daemon_spec in daemon_spec_list:
            node_list.append(daemon_spec['host'])
        self.service.del_nodes_from_depot(depot_id, node_list, False)
        daemons = self.service._depot_map[depot_id].get_daemon_list()
        self.assertEquals(len(self.service._depot_map[depot_id].get_daemon_list('mon')), 4)
        self.assertEquals(len(self.service._depot_map[depot_id].get_daemon_list('osd')), 4)
        self.assertEquals(len(self.service._depot_map[depot_id].get_daemon_list('mds')), 2)

        self.service.del_nodes_from_depot(depot_id, node_list, True)
        daemons = self.service._depot_map[depot_id].get_daemon_list()
        self.assertEquals(len(self.service._depot_map[depot_id].get_daemon_list('mon')), 1)
        self.assertEquals(len(self.service._depot_map[depot_id].get_daemon_list('osd')), 1)
        self.assertEquals(len(self.service._depot_map[depot_id].get_daemon_list('mds')), 0)

        self.service.add_daemons_to_depot(depot_id, daemon_spec_list)
        daemons = self.service._depot_map[depot_id].get_daemon_list()
        self.assertEquals(len(self.service._depot_map[depot_id].get_daemon_list('mon')), 4)
        self.assertEquals(len(self.service._depot_map[depot_id].get_daemon_list('osd')), 4)
        self.assertEquals(len(self.service._depot_map[depot_id].get_daemon_list('mds')), 2)

        self.assertTrue(self.service._depot_map[depot_id]._check_ceph_ids_are_consecutive())

if __name__ == '__main__':
    unittest.main()
import uuid
from depot import Depot

class TcdsService(object):
    _depot_map = {}
    utils = None
    def __init__(self, utils, varstore):
        self._depot_map = {}
        self.utils = utils
        self.var = varstore
        if varstore.PERSISTENT:
            self._load_saved_state()

    def _load_saved_state(self):
        depot_list = self.var.get_depot_list()
        for depot_spec in depot_list:
            depot = Depot(self, depot_spec['uuid'])
            self._depot_map[depot_spec['uuid']] = depot
            depot._load_saved_state()

    def create_depot(self, depot_id, replication_factor):
        depot = Depot(self, depot_id)
        self.var.add_depot(depot, depot_id, replication_factor, depot.CONSTANTS['STATE_OFFLINE'])
        depot.setup()
        self._depot_map[depot_id] = depot
        return depot

    def remove_depot(self, depot_id):
        self._depot_map[depot_id].deactivate()
        self._depot_map[depot_id].clean()
        self.var.del_depot(self._depot_map[depot_id])
        del self._depot_map[depot_id]

    def query_depot(self, depot_id):
        depot_info = self._depot_map[depot_id].get_info()
        return depot_info

    def add_daemons_to_depot(self, depot_id, daemon_spec_list):
        self._depot_map[depot_id].add_daemons(daemon_spec_list)
        return self._depot_map[depot_id].get_info()

    def del_nodes_from_depot(self, depot_id, node_list, force=False):
        self._depot_map[depot_id].remove_nodes(node_list, force)
        return self._depot_map[depot_id].get_info()


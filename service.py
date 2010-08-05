"""Classes pertaining to the control of the TCDS Service at the cluster level

Exports:
* TcdsService
"""
from depot import Depot

class TcdsService(object):
    """Top level class that controls and maintains the cluster wide state of
    the TCDS service."""
    _depot_map = {}
    utils = None
    var = None

    def __init__(self, utils, varstore):
        self._depot_map = {}
        self.utils = utils
        self.var = varstore
        if varstore.PERSISTENT:
            self._load_saved_state()

    def _load_saved_state(self):
        """Read and reproduce the state of the cluster from this instance's
        varstore."""
        depot_list = self.var.get_depot_list()
        for depot_spec in depot_list:
            depot = Depot(self, depot_spec['uuid'])
            self._depot_map[depot_spec['uuid']] = depot
            depot._load_saved_state()

    def create_depot(self, depot_id, replication_factor):
        """Creates a new depot

        @type  depot_id   string
        @param depot_id   the newly created depot will have this uuid
        @type  replication_factor   int
        @param replication_factor   the replication of the new depot

        @rtype  Depot instance
        @return the newly created depot
        """
        depot = Depot(self, depot_id)
        self.var.add_depot(depot, depot_id, replication_factor,
            depot.CONSTANTS['STATE_OFFLINE'])
        depot.setup()
        self._depot_map[depot_id] = depot
        return depot

    def remove_depot(self, depot_id):
        """Permanently delete a depot

        This function will shutdown all daemons within the depot and erase all
        traces of the depot in the varstore

        @type  depot_id   string
        @param depot_id   uuid of the depot to remove
        """
        self._depot_map[depot_id].delete()
        self.var.del_depot(self._depot_map[depot_id])
        del self._depot_map[depot_id]

    def query_depot(self, depot_id):
        """Retrieve information about a depot

        @type  depot_id   string
        @param depot_id   uuid of the depot to query

        @rtype  DepotInfo instance
        @return information about the depot in a DepotInfo instance (which is
                a subclass of dict). See the DepotInfo class documentation for
                the available fields.
        """
        depot_info = self._depot_map[depot_id].get_info()
        return depot_info

    def add_daemons_to_depot(self, depot_id, daemon_spec_list):
        """Create daemons under a depot

        Creates, registers, sets up, and if conditions allow, starts the given
        daemons under the depot.

        @type  daemon_spec_list list
        @param daemon_spec_list list of daemon specification dictionaries. Each
                    daemon specification should contain the fields 'type',
                    'host' and 'uuid'; where 'type' is a valid daemon type,
                    'host is the uuid of the daemon's host machine, and 'uuid'
                    is the uuid string of the new daemon

        @rtype  list
        @return a list containing the uuids of the daemons created
        """
        daemons_added = self._depot_map[depot_id].add_daemons(daemon_spec_list)
        added_daemons_uuids = []
        for daemon in daemons_added:
            added_daemons_uuids.append(daemon.uuid)
        return added_daemons_uuids

    def del_nodes_from_depot(self, depot_id, node_list, force=False):
        """Removes all daemons on the given host machines that belong to the
        given depot.

        By default, this operation will check that after performing the
        removal, the depot will remain healty (as per the definition in
        Depot._get_meets_min_requirements()). If the check fails, the
        opertaion will be aborted.

        @type  depot_id   string
        @param depot_id   the uuid of the depot to remove daemons from
        @type  node_list  list
        @param node_list  list of host machine uuid strings on which to remove
                          daemons
        @type  force      boolean
        @param force      if True, does not perform checks and will always
                          attempt to remove the target daemons

        @rtype  list
        @return a list containing the uuids of the daemons removed
        """
        daemons_removed = self._depot_map[depot_id].remove_nodes(node_list,
            force)
        removed_daemons_uuids = []
        for daemon in daemons_removed:
            removed_daemons_uuids.append(daemon.uuid)
        return removed_daemons_uuids



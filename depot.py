from tccephconf import TCCephConf
from daemon import Mon, Mds, Osd

class Depot(object):
    var = None
    CONSTANTS = {
        'STATE_OFFLINE': 0,
        'STATE_ONLINE': 1
    }
    def __init__(self, id, varstore, replication_factor=3, state=None):
        self.var = varstore
        self.var.set_depot_id(id)
        self.set_replication_factor(replication_factor)
        if state is None:
            state = self.CONSTANTS['STATE_OFFLINE']
        self.set_state(state)
    def __del__(self): pass
    def get_info(self):
        depot_info = {
            'depot_id': self.var.get_depot_id(),
            'depot_replication': self.var.get_replication_factor(),
            'depot_state': ['not ready', 'ready'][self.var.get_depot_state()],
            'depot_capacity': 0,
            'depot_usage': 0
        }
        return depot_info
        #TODO get actual usage

    def add_nodes(self, args):
        for node in args:
            if 'node_ip' in node:
                self.var.resolv[node['node_id']] = node['node_ip']
            for role in node['storage_roles']:
                if role == 'mon':
                    self._add_daemon(node['node_id'], Mon(self.var))
                elif role == 'mds':
                    self._add_daemon(node['node_id'], Mds(self.var))
                elif role == 'osd':
                    self._add_daemon(node['node_id'], Osd(self.var))

        daemon_count = self._get_daemon_count()
        if Depot._get_meets_min_requirements(replication=self.get_replication_factor(), **daemon_count):
            self.activate()
        else:
            print daemon_count, self.get_replication_factor()

    def remove_nodes(self, args):
        daemon_list = self.var.get_daemon_list()
        remove_pending = []
        for node in args:
            for daemon in daemon_list:
                print daemon.get_host_id(), node['node_id']
                if daemon.get_host_id() == node['node_id']:
                    remove_pending.append(daemon)
        self.var.remove_daemons(remove_pending)

    def get_state(self):
        return self.var.get_depot_state()

    def set_state(self, state):
        self.var.set_depot_state(state)

    def _add_daemon(self, host_id, daemon):
        self.var.add_daemon(daemon)
        self.var.set_daemon_host(daemon, host_id)

    def get_daemon_list(self):
        return self.var.get_daemon_list()

    def _get_daemon_count(self):
        daemon_list = self.get_daemon_list()
        daemon_count = {'num_mon': 0, 'num_mds':0, 'num_osd':0}
        for daemon in daemon_list:
            daemon_count['num_'+daemon.TYPE] = daemon_count['num_'+daemon.TYPE] + 1
        return daemon_count

    def set_replication_factor(self, factor):
        self.var.set_depot_replication_factor(factor)

    def get_replication_factor(self):
        return self.var.get_replication_factor()

    @staticmethod
    def _get_meets_min_requirements(replication, num_mon, num_mds, num_osd):
        if type(replication) is not int \
            or type(num_mon) is not int \
            or type(num_mds) is not int \
            or type(num_osd) is not int:
            raise TypeError('int arguments expected, got %s, %s, %s, %s' %
                (type(replication), type(num_mon), type(num_mds), type(num_osd)))
        if replication < 1 or num_mon < 0 or num_mds < 0 or num_osd < 0:
            raise ValueError
        if num_mon >= 3 and num_mds >= 2 and num_osd >= replication:
            return True
        else:
            return False

    def activate(self):
        config = TCCephConf()
        config.create_default(self.var.get_depot_id())

        daemon_list = self.get_daemon_list()
        next_id = {'mon': 0, 'mds': 0, 'osd': 0}
        for daemon in daemon_list:
            daemon.set_ceph_id(next_id[daemon.TYPE])
            daemon.add_to_config(config)
            next_id[daemon.TYPE] = next_id[daemon.TYPE] + 1

        with open('%s.conf' % self.var.get_depot_id(), 'wb') as config_file:
            config.write(config_file)
        for daemon in daemon_list:
            daemon.set_config(config)

        cmd = "mkcephfs -c %s --allhosts" % (config_file, )
        self._run_shell_command(cmd)

        # start all nodes
        for daemon in daemon_list:
            daemon.activate()

        # set replication factor
        replication = self.get_replication_factor()

        cmd = "ceph -c %s osd pool set metadata size %d" % (config_file, replication)
        self._run_shell_command(cmd)

        cmd = "ceph -c %s osd pool set data size %d" % (config_file, replication)
        self._run_shell_command(cmd)

        # Change Depot Status to live
        self.set_state(self.CONSTANTS['STATE_ONLINE'])

    def deactivate(self): pass
    @staticmethod
    def _run_shell_command(cmd): pass



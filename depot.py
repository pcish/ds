from tccephconf import TCCephConf

class VarStore(object):
    def get_depot_id(self): pass
    def set_depot_id(self, id): pass
    def get_depot_state(self): pass
    def set_depot_state(self, state): pass
    def add_daemon(self, daemon): pass
    def remove_daemon(self, daemon): pass
    def remove_daemons(self, daemon_list): pass
    def get_daemon_list(self): pass

class MultiVarStore(VarStore):
    pass

class LocalVarStore(VarStore):
    __depot_id = None
    __depot_state = None
    __depot_replication_factor = None
    __daemon_list = []
    resolv = {}

    def set_depot_id(self, depot_id):
        self.__depot_id = depot_id

    def get_depot_id(self):
        return self.__depot_id

    def set_depot_state(self, state):
        self.__depot_state = state

    def get_depot_state(self):
        return self.__depot_state

    def add_daemon(self, daemon):
        assert(self.__daemon_list.count(daemon) == 0)
        self.__daemon_list.append(daemon)

    def remove_daemon(self, daemon):
        self.__daemon_list.remove(daemon)
        assert(self.__daemon_list.count(daemon) == 0)

    def remove_daemons(self, daemon_list):
        for daemon in daemon_list:
            self.__daemon_list.remove(daemon)
            assert(self.__daemon_list.count(daemon) == 0)

    def get_daemon_list(self):
        return self.__daemon_list

    def set_daemon_host(self, daemon, daemon_id):
        self.__daemon_list[self.__daemon_list.index(daemon)].id = daemon_id

    def get_daemon_host(self, daemon):
        return self.__daemon_list[self.__daemon_list.index(daemon)].id

    def set_depot_replication_factor(self, factor):
        self.__depot_replication_factor = factor

    def get_replication_factor(self):
        return self.__depot_replication_factor

    def host_id_to_ip(self, host_id):
        if host_id in self.resolv:
            return self.resolv[host_id]
        else:
            return None

    def set_daemon_ceph_id(self, daemon, ceph_id):
        self.__daemon_list[self.__daemon_list.index(daemon)].ceph_id = ceph_id

    def get_daemon_ceph_id(self, daemon):
        return self.__daemon_list[self.__daemon_list.index(daemon)].ceph_id


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
            self.start()
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

    def start(self):
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
            daemon.start()

        # set replication factor
        replication = self.get_replication_factor()

        cmd = "ceph -c %s osd pool set metadata size %d" % (config_file, replication)
        self._run_shell_command(cmd)

        cmd = "ceph -c %s osd pool set data size %d" % (config_file, replication)
        self._run_shell_command(cmd)

        # Change Depot Status to live
        self.set_state(self.CONSTANTS['STATE_ONLINE'])

    def stop(self): pass
    @staticmethod
    def _run_shell_command(cmd): pass


class Daemon:
    var = None
    DAEMON_NAME = None
    conf_file_path = None
    INIT_SCRIPT = '/etc/init.d/ceph'
    TYPE = None
    def __init__(self, varstore):
        self.var = varstore

    def get_host_id(self):
        return self.var.get_daemon_host(self)

    def get_host_ip(self):
        return self.var.host_id_to_ip(self.get_host_id())

    def set_ceph_id(self, id):
        self.var.set_daemon_ceph_id(self, id)

    def get_ceph_id(self):
        return self.var.get_daemon_ceph_id(self)

    def add_to_config(self, config): pass

    def _run_remote_command(self, command):
        pass
    def prepare(self):
        pass
    def start(self):
        cmd = "%s -c %s --hostname %s start %s" % (self.INIT_SCRIPT, self.conf_file_path, self.get_host_ip(), self.TYPE)
        self._run_remote_command(cmd)

    def status(self):
        pass
    def stop(self):
        pass
    def getDaemonArgs(self):
        return '-c %s' % self.conf_file_path

    def set_config(self, config):
        self.conf_file_path = config.get('tcloud', 'depot')



class Osd(Daemon):
    DAEMON_NAME = 'cosd'
    TYPE = 'osd'
    def getDaemonArgs(self):
        return '%s -i %d' % (super().getDaemonArgs(), self.get_ceph_id())

    def add_to_config(self, config):
        config.add_mon(self.get_ceph_id(), self.get_host_ip())


class Mds(Daemon):
    DAEMON_NAME = 'cmds'
    TYPE = 'mds'
    def add_to_config(self, config):
        config.add_mds(self.get_ceph_id(), self.get_host_ip())


class Mon(Daemon):
    DAEMON_NAME = 'cmon'
    TYPE = 'mon'
    def getDaemonArgs(self):
        return '%s -i %s' % (super().getDaemonArgs(), self.ceph_id)

    def add_to_config(self, config):
        config.add_osd(self.get_ceph_id(), self.get_host_ip())

    def prepare(self):
        # add monitor to the mon map
        cmd = 'ceph -c %s mon add %s %s:6789' % (self.conf_file_path, self.get_host_id(), self.get_host_ip())
        Depot._run_shell_command(cmd)

        # copy mon dir from an existing to the new monitor
        cmd = 'scp -r %s:%s/mon%d /tmp/mon' %  \
                (args['monitorIP'], depot_conf_constants['MON_DATA_PREFIX'], args['monitorID'])
        Depot._run_shell_command(cmd)

        cmd = 'scp -r /tmp/mon %s:/tmp/mon%d' %  \
                (info['IP'], info['cephid'])
        Depot._run_shell_command(cmd)

        cmd = 'cp -r /tmp/mon%d %s' % (info['cephid'], depot_conf_constants['MON_DATA_PREFIX'])
        self._run_remote_command(info['IP'], cmd)

        _DeployConfigrationFile(info['DepotID'])

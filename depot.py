import logging
import os

from tccephconf import TCCephConf
from daemon import Mon, Mds, Osd

class Depot(object):
    service_globals = None
    var = None
    CONSTANTS = {
        'STATE_OFFLINE': 0,
        'STATE_ONLINE': 1
    }
    config_file_path = None
    config = None

    def __init__(self, service_globals, id, varstore, replication_factor=3, state=None):
        self.service_globals = service_globals
        self.var = varstore
        self.var.set_depot_id(id)
        self.var.set_replication_factor(replication_factor)
        if state is None:
            state = self.CONSTANTS['STATE_OFFLINE']
        self.set_state(state)
        #self.config_file_path = '/etc/ceph/%s.conf' % id
        self.config_file_path = '%s.conf' % id
        self.config = TCCephConf()
        self.config.create_default(self.var.get_depot_id())
        print "new depot"

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

    def add_nodes(self, node_list):
        for node in node_list:
            if 'node_ip' in node:
                self.var.resolv[node['node_id']] = node['node_ip']
            for role in node['storage_roles']:
                if role == 'mon':
                    self._add_daemon(Mon(self, node['node_id'], self._get_next_ceph_name_for(role)))
                elif role == 'mds':
                    self._add_daemon(Mds(self, node['node_id'], self._get_next_ceph_name_for(role)))
                elif role == 'osd':
                    self._add_daemon(Osd(self, node['node_id'], self._get_next_ceph_name_for(role)))

        daemon_count = self._get_daemon_count()
        if self.get_state() == self.CONSTANTS['STATE_OFFLINE']:
            if Depot._get_meets_min_requirements(replication=self.var.get_replication_factor(), **daemon_count):
                self.activate()
            else:
                self.service_globals.dout(logging.DEBUG, '%s %s' % (daemon_count, self.var.get_replication_factor()))

    def remove_nodes(self, node_list, force=False):
        daemon_list = self.var.get_daemon_list()
        daemon_count = self._get_daemon_count()
        remove_pending = []
        for node in node_list:
            for daemon in daemon_list:
                if daemon.get_host_id() == node['node_id']:
                    remove_pending.append(daemon)
                    daemon_count['num_'+daemon.TYPE] = daemon_count['num_'+daemon.TYPE] - 1

        if force or self._get_meets_min_requirements(replication=self.var.get_replication_factor(), **daemon_count):
            for daemon in remove_pending:
                daemon.deactivate()
                daemon.del_from_config(self.config)
            self.var.remove_daemons(remove_pending)

    def get_state(self):
        return self.var.get_depot_state()

    def set_state(self, state):
        self.var.set_depot_state(state)

    def _add_daemon(self, daemon):
        self.var.add_daemon(daemon)

        if self.get_state() == self.CONSTANTS['STATE_ONLINE']:
            daemon.add_to_config(self.config)
            daemon.set_config(self.config)
            daemon.setup()
            if daemon.TYPE == 'mon':
                # add monitor to the mon map
                cmd = 'ceph -c %s mon add %s %s:6789' % (self.config_file_path, daemon.get_host_id(), daemon.get_host_ip())
                self.service_globals.run_shell_command(cmd)
                # copy mon dir from an existing to the new monitor
                (active_mon_ip, active_mon_id) = self.config.get_active_mon_ip()
                cmd = 'scp -r %s:%s/mon%s %s:%s' %  \
                        (active_mon_ip, os.path.dirname(self.config.get('mon', 'mon data')), active_mon_id,
                        daemon.get_host_ip(), os.path.dirname(self.config.get('mon', 'mon data'))
                        )
                self.service_globals.run_shell_command(cmd)
            elif daemon.TYPE == 'osd':
                # set max osd
                # NB: we assume that the osd id's are monotonically increasing
                cmd = 'ceph -c %s osd setmaxosd %s' % (self.config_file_path, daemon.get_ceph_id() + 1)
                self.service_globals.run_shell_command(cmd)

                # update crush map
                cmd = 'osdmaptool --createsimple %s --clobber /tmp/osdmap.junk --export-crush /tmp/crush.new' % (daemon.get_ceph_id() + 1)
                self.service_globals.run_shell_command(cmd)

                cmd = 'ceph osd setcrushmap -i /tmp/crush.new'
                self.service_globals.run_shell_command(cmd)

            daemon.activate()


    def get_daemon_list(self, type='all'):
        return self.var.get_daemon_list(type)

    def _get_daemon_count(self):
        daemon_list = self.get_daemon_list()
        daemon_count = {'num_mon': 0, 'num_mds':0, 'num_osd':0}
        for daemon in daemon_list:
            daemon_count['num_'+daemon.TYPE] = daemon_count['num_'+daemon.TYPE] + 1
        return daemon_count

    def set_replication_factor(self, factor):
        assert(self.config_file_path is not None)
        cmd = "ceph -c %s osd pool set metadata size %s" % (self.config_file_path, factor)
        self.service_globals.run_shell_command(cmd)

        cmd = "ceph -c %s osd pool set data size %s" % (self.config_file_path, factor)
        self.service_globals.run_shell_command(cmd)

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

    def _get_next_ceph_name_for(self, daemon_type):
        daemon_list = self.get_daemon_list(daemon_type)
        in_use_names = []
        for daemon in daemon_list:
            in_use_names.append(daemon.get_ceph_id())
        in_use_names.sort()
        for i in range(len(in_use_names)):
            if in_use_names[i] != i:
                return i
        return len(in_use_names)

    def _check_ceph_ids_are_consequtive(self):
        daemon_list = self.get_daemon_list()
        daemon_list.sort()
        next_id = {'mon': 0, 'mds': 0, 'osd': 0}
        for daemon in daemon_list:

            if next_id[daemon.TYPE] == daemon.get_ceph_id():
                next_id[daemon.TYPE] = next_id[daemon.TYPE] + 1
            else:
                return False
        return True

    def activate(self):


        daemon_list = self.get_daemon_list()
        next_id = {'mon': 0, 'mds': 0, 'osd': 0}
        for daemon in daemon_list:
            daemon.set_ceph_id(next_id[daemon.TYPE])
            daemon.add_to_config(self.config)
            next_id[daemon.TYPE] = next_id[daemon.TYPE] + 1

        assert(self.config_file_path is not None)
        with open(self.config_file_path, 'wb') as config_file:
            self.config.write(config_file)
        for daemon in daemon_list:
            daemon.set_config(self.config)

        cmd = "mkcephfs -c %s --allhosts" % (self.config_file_path, )
        self.service_globals.run_shell_command(cmd)

        for daemon in daemon_list:
            daemon.activate()

        self.set_replication_factor(self.var.get_replication_factor())
        self.set_state(self.CONSTANTS['STATE_ONLINE'])

    def deactivate(self): pass




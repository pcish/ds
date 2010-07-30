import ConfigParser
import os

class Daemon(object):
    _localvars = None
    depot = None

    DAEMON_NAME = None
    conf_file_path = None
    config = None
    INIT_SCRIPT = '/etc/init.d/ceph'
    TYPE = None

    def __init__(self, depot):
        self.depot = depot
        self._localvars = {} # TODO: check that we need to init this

    def set_uuid(self, uuid):
        self.depot.var.set_daemon_uuid(self, uuid)

    def get_uuid(self):
        return self.depot.var.get_daemon_uuid(self)

    def get_host_id(self):
        return self.depot.var.get_daemon_host(self)

    def get_host_ip(self):
        return self.depot.var.host_id_to_ip(self.get_host_id())

    def set_ceph_name(self, name):
        self.depot.var.set_daemon_ceph_name(self, name)

    def get_ceph_name(self):
        return self.depot.var.get_daemon_ceph_name(self)

    def add_to_config(self, config): assert 0, 'virtual function called'
    def del_from_config(self, config):
        config.del_daemon(self)

    def set_config(self, config):
        self.conf_file_path = '%s.conf' % config.get('tcloud', 'depot')
        self.config = config

    def setup(self):
        pass

    def activate(self):
        cmd = "%s -c %s --hostname %s start %s" % (self.INIT_SCRIPT, self.conf_file_path, self.get_host_ip(), self.TYPE)
        self.depot.service_globals.run_remote_command(self.get_host_ip(), cmd)

    def status(self):
        pass

    def deactivate(self):
        cmd = "%s -c %s --hostname %s stop %s" % (self.INIT_SCRIPT, self.conf_file_path, self.get_host_ip(), self.TYPE)
        self.depot.service_globals.run_remote_command(self.get_host_ip(), cmd)

    def clean(self):
        pass

    def getDaemonArgs(self):
        return '-c %s' % self.conf_file_path


class Osd(Daemon):
    DAEMON_NAME = 'cosd'
    TYPE = 'osd'
    def getDaemonArgs(self):
        return '%s -i %d' % (super().getDaemonArgs(), self.get_ceph_name())

    def add_to_config(self, config):
        config.add_osd(self, self.get_host_ip())

    def setup(self):
        cmd = "mkdir -p %s" % (os.path.dirname(self.config.get('osd', 'osd data')),)
        self.depot.service_globals.run_remote_command(self.get_host_ip(), cmd)
        cmd = "mkdir -p %s" % (os.path.dirname(self.config.get('osd', 'osd journal')),)
        self.depot.service_globals.run_remote_command(self.get_host_ip(), cmd)

        # get a copy of monmap
        cmd = 'ceph -c %s mon getmap -o /tmp/monmap' % (self.conf_file_path,)
        self.depot.service_globals.run_remote_command(self.get_host_ip(), cmd)

        # formatting new osd
        cmd = '"cosd -c %s -i %d --mkfs --monmap /tmp/monmap"' % (self.conf_file_path, self.get_ceph_name())
        self.depot.service_globals.run_remote_command(self.get_host_ip(), cmd)


class Mds(Daemon):
    DAEMON_NAME = 'cmds'
    TYPE = 'mds'
    def add_to_config(self, config):
        config.add_mds(self, self.get_host_ip())

class Mon(Daemon):
    DAEMON_NAME = 'cmon'
    TYPE = 'mon'
    def getDaemonArgs(self):
        return '%s -i %s' % (super().getDaemonArgs(), self.get_ceph_name())

    def add_to_config(self, config):
        config.add_mon(self, self.get_host_ip())

    def setup(self):
        cmd = 'mkdir -p %s' % (os.path.dirname(self.config.get('mon', 'mon data')),)
        self.depot.service_globals.run_remote_command(self.get_host_ip(), cmd)

        # copy mon dir from an existing to the new monitor
        (active_mon_ip, active_mon_id) = self.config.get_active_mon_ip()
        cmd = 'scp -r %s:%s/mon%s %s:%s' %  \
                (active_mon_ip, os.path.dirname(self.config.get('mon', 'mon data')), active_mon_id,
                self.get_host_ip(), os.path.dirname(self.config.get('mon', 'mon data'))
                )
        self.depot.service_globals.run_shell_command(cmd)

    def deactivate(self):
        cmd = 'ceph -c %s mon remove %s' % (self.conf_file_path, self.get_ceph_name())
        self.depot.service_globals.run_shell_command(cmd)



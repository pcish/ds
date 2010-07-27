import ConfigParser

class Daemon:
    service_globals = None
    var = None
    DAEMON_NAME = None
    conf_file_path = None
    config = None
    INIT_SCRIPT = '/etc/init.d/ceph'
    TYPE = None
    def __init__(self, service_globals, varstore):
        self.service_globals = service_globals
        self.var = varstore

    def get_host_id(self):
        return self.var.get_daemon_host(self)

    def get_host_ip(self):
        return self.var.host_id_to_ip(self.get_host_id())

    def set_ceph_id(self, id):
        self.var.set_daemon_ceph_id(self, id)

    def get_ceph_id(self):
        return self.var.get_daemon_ceph_id(self)

    def add_to_config(self, config): assert(0)

    def set_config(self, config):
        self.conf_file_path = '%s.conf' % config.get('tcloud', 'depot')
        self.config = config

    def setup(self):
        pass

    def activate(self):
        cmd = "%s -c %s --hostname %s start %s" % (self.INIT_SCRIPT, self.conf_file_path, self.get_host_ip(), self.TYPE)
        self.service_globals.run_remote_command(self.get_host_ip(), cmd)

    def status(self):
        pass

    def deactivate(self):
        cmd = "%s -c %s --hostname %s stop %s" % (self.INIT_SCRIPT, self.conf_file_path, self.get_host_ip(), self.TYPE)
        self.service_globals.run_remote_command(self.get_host_ip(), cmd)

    def clean(self):
        pass

    def getDaemonArgs(self):
        return '-c %s' % self.conf_file_path

    @staticmethod
    def get_active_mon_ip_from_config(config):
        active_mon_ip = None
        i = 0
        while active_mon_ip is None:
            try:
                addr_string = config.get('mon.%s' % i, 'addr')
            except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
                i = i + 1
            else:
                (active_mon_ip, _, _) = addr_string.partition(':')
        return (active_mon_ip, '%s' % i)



class Osd(Daemon):
    DAEMON_NAME = 'cosd'
    TYPE = 'osd'
    def getDaemonArgs(self):
        return '%s -i %d' % (super().getDaemonArgs(), self.get_ceph_id())

    def add_to_config(self, config):
        config.add_mon(self.get_ceph_id(), self.get_host_ip())

    def setup(self):
        cmd = "mkdir -p %s" % (os.path.dirname(self.config.get('osd', 'osd data')),)
        self.service_globals.run_remote_command(self.get_host_ip(), cmd)
        cmd = "mkdir -p %s" % (os.path.dirname(self.config.get('osd', 'osd journal')),)
        self.service_globals.run_remote_command(self.get_host_ip(), cmd)

        # get a copy of monmap
        cmd = 'ceph -c %s mon getmap -o /tmp/monmap' % (self.conf_file_path,)
        self.service_globals.run_remote_command(self.get_host_ip(), cmd)

        # formatting new osd
        cmd = '"cosd -c %s -i %d --mkfs --monmap /tmp/monmap"' % (self.conf_file_path, self.get_ceph_id())
        self.service_globals.run_remote_command(self.get_host_ip(), cmd)


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

    def setup(self):
        cmd = 'mkdir -p %s' % (os.path.dirname(self.config.get('mon', 'mon data')),)
        self.service_globals.run_remote_command(self.get_host_ip(), cmd)

    def deactivate(self):
        cmd = 'ceph -c %s mon remove %s' % (self.conf_file_path, self.get_ceph_id())
        self.service_globals.run_shell_command(cmd)



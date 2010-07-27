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

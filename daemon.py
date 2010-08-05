import os

class Daemon(object):
    depot = None
    uuid = None
    utils = None

    DAEMON_NAME = None
    conf_file_path = None
    config = None
    INIT_SCRIPT = '/etc/init.d/ceph'
    TYPE = None

    def __init__(self, depot, uuid):
        self.depot = depot
        self.uuid = uuid
        self.utils = depot.utils

    def _load_saved_state(self):
        self.conf_file_path = os.path.join(self.utils.CONFIG_FILE_PATH_PREFIX, '%s.conf' % self.depot.uuid)

    @staticmethod
    def cmp_name(first, second):
        if first.TYPE == second.TYPE:
            first_name = first.get_ceph_name()
            second_name = second.get_ceph_name()
            if first_name.isdigit() and second_name.isdigit():
                return cmp(int(first_name), int(second_name))
            else:
                return cmp(first_name, second_name)
        else:
            return cmp(first.TYPE, second.TYPE)

    def set_uuid(self, uuid):
        self.depot.var.set_daemon_uuid(self, uuid)

    def get_uuid(self):
        return self.uuid

    def get_host_id(self):
        return self.depot.var.get_daemon_host(self)

    def get_host_ip(self):
        return self.utils.resolv.uuid_to_ip(self.get_host_id())

    def set_ceph_name(self, name):
        self.depot.var.set_daemon_ceph_name(self, name)

    def get_ceph_name(self):
        return self.depot.var.get_daemon_ceph_name(self)

    def add_to_config(self, config): assert 0, 'virtual function called'

    def del_from_config(self, config):
        config.del_daemon(self)

    def set_config(self, config):
        self.conf_file_path = os.path.join(self.utils.CONFIG_FILE_PATH_PREFIX,
            '%s.conf' % config.get('tcloud', 'depot'))
        self.config = config

    def write_config(self):
        tmp_file_path = '/tmp/%s.conf' % self.config.get('tcloud', 'depot')
        with open(tmp_file_path, 'wb') as tmp_file:
            self.config.write(tmp_file)
        cmd = 'scp %s %s:%s' % (tmp_file_path, self.get_host_ip(),
            self.conf_file_path)
        self.utils.run_shell_command(cmd)

    def setup(self, config):
        self.set_config(config)
        cmd = 'mkdir -p /var/log/ceph'
        self.utils.run_remote_command(self.get_host_ip(), cmd)
        cmd = 'mkdir -p /var/run/ceph'
        self.utils.run_remote_command(self.get_host_ip(), cmd)

    def activate(self):
        cmd = 'service ntpdate start'
        self.utils.run_remote_command(self.get_host_ip(), cmd)
        self.write_config()
        cmd = "%s -c %s --hostname %s start %s" % (self.INIT_SCRIPT,
            self.conf_file_path, self.get_host_ip(), self.TYPE)
        self.utils.run_remote_command(self.get_host_ip(), cmd)

    def status(self):
        pass

    def deactivate(self):
        cmd = "%s -c %s --hostname %s stop %s" % (self.INIT_SCRIPT,
            self.conf_file_path, self.get_host_ip(), self.TYPE)
        self.utils.run_remote_command(self.get_host_ip(), cmd)

    def delete(self):
        pass


class Osd(Daemon):
    DAEMON_NAME = 'cosd'
    TYPE = 'osd'

    def add_to_config(self, config):
        config.add_osd(self, self.get_host_ip())

    def setup(self, config):
        super(Osd, self).setup(config)
        cmd = "mkdir -p %s" % (self.config.get('osd', 'osd data'),)
        cmd = cmd.replace('$id', self.get_ceph_name())
        self.utils.run_remote_command(self.get_host_ip(), cmd)
        cmd = "mkdir -p %s" % (
            os.path.dirname(self.config.get('osd', 'osd journal')),)
        self.utils.run_remote_command(self.get_host_ip(), cmd)

        self.write_config()
        # get a copy of the monmap
        cmd = 'ceph -c %s mon getmap -o /tmp/monmap' % (self.conf_file_path,)
        self.utils.run_remote_command(self.get_host_ip(), cmd)

        # format the new osd data dir
        cmd = 'cosd -c %s -i %s --mkfs --monmap /tmp/monmap' % (
            self.conf_file_path, self.get_ceph_name())
        self.utils.run_remote_command(self.get_host_ip(), cmd)

    def delete(self):
        cmd = 'ceph -c %s osd out %s' % (self.conf_file_path,
            self.get_ceph_name())
        self.utils.run_shell_command(cmd)
        cmd = 'ceph -c %s osd down %s' % (self.conf_file_path,
            self.get_ceph_name())
        self.utils.run_shell_command(cmd)
        self.deactivate()


class Mds(Daemon):
    DAEMON_NAME = 'cmds'
    TYPE = 'mds'
    def add_to_config(self, config):
        config.add_mds(self, self.get_host_ip())

    def delete(self):
        cmd = 'ceph -c %s mds stop %s' % (self.conf_file_path,
            self.get_ceph_name())
        self.utils.run_shell_command(cmd)

class Mon(Daemon):
    DAEMON_NAME = 'cmon'
    TYPE = 'mon'

    def add_to_config(self, config):
        config.add_mon(self, self.get_host_ip())

    def setup(self, config):
        super(Mon, self).setup(config)

        # copy mon data dir from an existing monitor
        (active_mon_ip, active_mon_id) = self.config.get_active_mon_ip()
        self_ip = self.get_host_ip()
        cmd = 'mkdir -p %s' % self.config.get('mon', 'mon data').replace('$id',
            self.get_ceph_name())
        self.utils.run_remote_command(self_ip, cmd)
        cmd = 'rm -rf %s' % \
            self.config.get('mon', 'mon data').replace('$id', active_mon_id)
        self.utils.run_remote_command(self_ip, cmd)
        cmd = 'rm -rf %s' % \
            self.config.get('mon', 'mon data').replace('$id',
            self.get_ceph_name())
        self.utils.run_remote_command(self_ip, cmd)
        cmd = 'scp -r %s:%s %s:%s' % (
            active_mon_ip, self.config.get('mon', 'mon data').replace('$id',
            active_mon_id), self_ip,
            os.path.dirname(self.config.get('mon', 'mon data'))
            )
        self.utils.run_shell_command(cmd)

        cmd = 'mv %s %s' % (
            self.config.get('mon', 'mon data').replace('$id', active_mon_id),
            self.config.get('mon', 'mon data').replace('$id',
            self.get_ceph_name()))
        self.utils.run_remote_command(self_ip, cmd)

    def delete(self):
        cmd = 'ceph -c %s mon remove %s' % (self.conf_file_path,
            self.get_ceph_name())
        self.utils.run_shell_command(cmd)


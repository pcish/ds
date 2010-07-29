import ConfigParser
import sys
import string

class CephConfFile(object):
    __fd = None
    def __init__(self, filename):
        self.__fd = open(filename)
    def readline(self, size=-1):
        line = '\n'
        while line != '' and line.lstrip() == '':
            line = self.__fd.readline(size)
        return line.lstrip()

    def __del__(self):
        self.__fd.close()

class TCCephConf(ConfigParser.RawConfigParser):
    DEFAULTS = {
        'MON_DATA': '/data/ceph/mon$id',
        'PID_FILE': '/var/run/ceph/$name.pid',
        'OSD_DATA': '/spare/osd$id',
        'OSD_JOURNAL': '/data/ceph/osdjournal/osd$id'
    }
    def get(self, section, option):
        try:
            return ConfigParser.RawConfigParser.get(self, section, option)
        except ConfigParser.NoOptionError as e:
            if section.startswith(('mon','mds','osd')):
                if section.__len__() > 3:
                    return self.get(section[:3], option)
                elif section.__len__() == 3:
                    return self.get('global', option)
            raise e

    def create_default(self, depot_id=None):
        self.add_section('tcloud')
        if depot_id is not None:
            self.set('tcloud', 'depot', '%s' % depot_id)
        self.add_section('global')
        self.set('global', 'pid file', self.DEFAULTS['PID_FILE'])
        self.set('global', 'folder quota', '1')
        self.add_section('mon')
        self.set('mon', 'mon data', self.DEFAULTS['MON_DATA'])
        self.set('mon', 'mon lease wiggle room', '1.000')
        self.add_section('mds')
        self.add_section('osd')
        self.set('osd', 'osd data', self.DEFAULTS['OSD_DATA'])
        self.set('osd', 'osd journal', self.DEFAULTS['OSD_JOURNAL'])
        self.set('osd', 'osd journal size', '100')
        self.add_section('group everyone')
        self.set('group everyone', 'addr', '0.0.0.0/0')
        self.add_section('mount /')
        self.set('mount /', 'allow', '%everyone')

    def add_mon(self, id, hostname, ip=None, port=None):
        if ip is None:
            ip = hostname
        if port is None:
            port = 6789
        mon_name = 'mon.%s' % id
        self.add_section(mon_name)
        self.set(mon_name, 'host', '%s' % hostname)
        self.set(mon_name, 'addr', '%s:%s' % (ip, port))

    def add_mds(self, id, hostname):
        mds_name = 'mds.%s' % id
        self.add_section(mds_name)
        self.set(mds_name, 'host', '%s' % hostname)

    def add_osd(self, id, hostname):
        osd_name = 'osd.%s' % id
        self.add_section(osd_name)
        self.set(osd_name, 'host', '%s' % hostname)

    def del_mon(self, id):
        self.remove_section('mon.%s' % id)

    def del_mds(self, id):
        self.remove_section('mds.%s' % id)

    def del_osd(self, id):
        self.remove_section('osd.%s' % id)

    def set_debug_all(self, debug_level):
        self.set('global', 'debug', '%s' % debug_level)
        self.set('osd', 'debug osd', '%s' % debug_level)
        self.set('mds', 'debug mds', '%s' % debug_level)
        self.set('mon', 'debug mon', '%s' % debug_level)
        self.set('global', 'debug ms', '%s' % debug_level)

    def get_active_mon_ip(self):
        sections = self.sections()
        sections.sort()
        for i in range(sections.__len__()):
            if sections[i].startswith('mon.') and sections[i].__len__() > 4:
                (active_mon_ip, _, _) = self.get(sections[i], 'addr').partition(':')
                break
        return (active_mon_ip, '%s' % sections[i][4:])

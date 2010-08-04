"""Classes related to manipulating ceph.conf files

Exports:
* CephConfFile
* TCCephConf
"""
import ConfigParser

class CephConfFile(object):
    """Helper class to read human modified ceph.conf files

    Strips leading whitespace from lines when reading in the file so that
    ConfigParser can parse it.
    """
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
    """In-memory representation of a ceph.conf file"""
    DEFAULTS = {
        'MON_DATA': '/data/ceph/mon$id',
        'PID_FILE': '/var/run/ceph/$name.pid',
        'OSD_DATA': '/spare/osd$id',
        'OSD_JOURNAL': '/data/ceph/osdjournal/osd$id'
    }
    def get(self, section, option):
        """Return the value of the option as it is found in section

        Overwrites the parent class's implementation by searching in 'parent'
        sections if the option is not found in the given section - emulating
        cconf's behaviour
        """
        try:
            return ConfigParser.RawConfigParser.get(self, section, option)
        except ConfigParser.NoOptionError as e:
            if section.startswith(('mon', 'mds', 'osd')):
                if section.__len__() > 3:
                    return self.get(section[:3], option)
                elif section.__len__() == 3:
                    return self.get('global', option)
            raise e

    def create_default(self, depot_id=None):
        """Populates this instance with default values

        @type  depot_id  string or uuid instance
        @param depot_id  uuid of the depot that this config file belongs to
        """
        self.add_section('tcloud')
        if depot_id is not None:
            self.set('tcloud', 'depot', '%s' % depot_id)
        self.add_section('global')
        self.set('global', 'pid file', self.DEFAULTS['PID_FILE'])
        self.set('global', 'folder quota', '0')
        self.add_section('mon')
        self.set('mon', 'mon data', self.DEFAULTS['MON_DATA'])
        self.set('mon', 'mon lease wiggle room', '2.000')
        self.add_section('mds')
        self.add_section('osd')
        self.set('osd', 'osd data', self.DEFAULTS['OSD_DATA'])
        self.set('osd', 'osd journal', self.DEFAULTS['OSD_JOURNAL'])
        self.set('osd', 'osd journal size', '100')
        self.add_section('group everyone')
        self.set('group everyone', 'addr', '0.0.0.0/0')
        self.add_section('mount /')
        self.set('mount /', 'allow', '%everyone')

    def add_mon(self, daemon, hostname, ipaddr=None, port=None):
        """Add a monitor to this config instance

        @type  daemon   Mon instance
        @param daemon   the Mon instance to add
        @type  hostname string
        @param hostname the hostname that this Mon resides on
        @type  ipaddr   string
        @param ipaddr   the IP address of the host that this Mon resides on
        @type  port     int or string
        @param port     the port that this Mon listens on
        """
        if ipaddr is None:
            ipaddr = hostname
        if port is None:
            port = 6789
        section_name = self._get_section(daemon)
        self.add_section(section_name)
        self.set(section_name, 'host', '%s' % hostname)
        self.set(section_name, 'mon addr', '%s:%s' % (ipaddr, port))

    def add_mds(self, daemon, hostname):
        """Add a mds to this config instance

        @type  daemon   Mds instance
        @param daemon   the Mds instance to add
        @type  hostname string
        @param hostname the hostname that this Mds resides on
        """
        section_name = self._get_section(daemon)
        self.add_section(section_name)
        self.set(section_name, 'host', '%s' % hostname)

    def add_osd(self, daemon, hostname):
        """Add a osd to this config instance

        @type  daemon   Osd instance
        @param daemon   the Osd instance to add
        @type  hostname string
        @param hostname the hostname that this Osd resides on
        """
        section_name = self._get_section(daemon)
        self.add_section(section_name)
        self.set(section_name, 'host', '%s' % hostname)

    def del_daemon(self, daemon):
        """Delete a daemon's section from this config instance

        @type  daemon   Daemon instance
        @param daemon   the Daemon instance to remove
        """
        self.remove_section(self._get_section(daemon))

    @staticmethod
    def _get_section(daemon):
        """Returns the name of the section that represents the given daemon

        @type  daemon   Daemon instance
        @param daemon   the Daemon to lookup

        @rtype   string
        @return  the section name of the daemon
        """
        if daemon.TYPE == 'osd':
            section_str = '%s%s' % (daemon.TYPE, daemon.get_ceph_name())
        else:
            section_str = '%s.%s' % (daemon.TYPE, daemon.get_ceph_name())
        return section_str

    def set_debug_all(self, debug_level):
        """Set the debugging level of various sections to the input value

        @type  debug_level  int
        @param debug_level  verbosity of the debugging messages (0~30)
        """
        self.set('global', 'debug', '%s' % debug_level)
        self.set('osd', 'debug osd', '%s' % debug_level)
        self.set('mds', 'debug mds', '%s' % debug_level)
        self.set('mon', 'debug mon', '%s' % debug_level)
        self.set('global', 'debug ms', '%s' % debug_level)

    def get_active_mon_ip(self):
        """Return the IP address of a monitor

        @rtype  string
        @return IP address of a monitor found in this config instance
        """
        sections = self.sections()
        sections.sort()
        for i in range(sections.__len__()):
            if sections[i].startswith('mon.') and sections[i].__len__() > 4:
                (active_mon_ip, _, _) = self.get(sections[i], 'mon addr').partition(':')
                break
        return (active_mon_ip, '%s' % sections[i][4:])

    def __str__(self):
        ret_str = ''
        for section in sorted(self.sections()):
            ret_str += '[%s]\n' % section
            for k, v in self.items(section):
                ret_str += '%s = %s\n' % (k, v)
            ret_str += '\n'
        return ret_str

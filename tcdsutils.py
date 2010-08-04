"""Collection of utility functions and global variables used by the ds package

Exports that are for external use:
* TcServiceUtils
* Tcdb
* TcdbResolv
"""
import subprocess
import logging
import inspect
import os

class ServiceUtils(object):
    """Base (abstract) class that defines global variables and the
    ServiceUtils interface"""
    @property
    def SUCCESS(self):
        """Return value used internally to indicate success"""
        return 0

    @property
    def ERROR_GENERAL(self):
        """Return value used internally to indicate failure"""
        return 1

    __resolv = None
    @property
    def resolv(self):
        """Class object that provides the Resolv interface (which looks up IP from
        host uuid's"""
        return self.__resolv

    __config_file_path_prefix = ''
    @property
    def CONFIG_FILE_PATH_PREFIX(self):
        """Directory in which the ceph configuration files are stored"""
        return self.__config_file_path_prefix

    def __init__(self, resolv=None):
        self.__resolv = resolv
    def get_libceph(self, config_file_path): return None
    def dout(self, level, message): pass
    def error_code(self, errorno): pass
    def run_shell_command(self, command): pass
    def run_remote_command(self, remote_host, command): pass
    def _format_dout_message(self, message):
        caller = inspect.stack()[2]
        return '[%s line %s] %s: %s' % (os.path.basename(caller[1]), caller[2], caller[3], message)


class LocalDebugServiceUtils(ServiceUtils):
    def dout(self, level, message):
        print self._format_dout_message(message)

    def error_code(self, errorno):
        return errorno

    def run_shell_command(self, command):
        print command

    def run_remote_command(self, remote_host, command):
        print 'ssh %s %s' % (remote_host, command)


class LocalUnittestServiceUtils(ServiceUtils):
    shell_commands = None

    def __init__(self, resolv):
        ServiceUtils.__init__(self, resolv)
        self.shell_commands = []

    def dout(self, level, message):
        pass

    def error_code(self, errorno):
        return errorno

    def run_shell_command(self, command):
        self.shell_commands.append(command)

    def run_remote_command(self, remote_host, command):
        self.shell_commands.append('ssh %s %s' % (remote_host, command))

    def clear_shell_commands(self):
        self.shell_commands = []


class TcServiceUtils(ServiceUtils):
    _logger = None
    _error_code_map = None

    def __init__(self, resolv):
        ServiceUtils.__init__(self, resolv)
        self.__config_file_path_prefix = '/etc/ceph'
        exec 'from tcloud.util.logger import TCLog'
        self._logger = TCLog('tcdsService')
        exec 'from tcloud.util.errorcode import TC_DISTRIBUTED_STORAGE_ERROR, TC_SUCCESS'
        self._error_code_map = {}
        self._error_code_map[str(self.SUCCESS)] = TC_SUCCESS
        self._error_code_map[str(self.ERROR_GENERAL)] = TC_DISTRIBUTED_STORAGE_ERROR

    def get_libceph(self, config_file_path):
        exec 'from tcloud.ds.ceph.libceph import LibCeph'
        return LibCeph(['', '-c', '%s' % config_file_path])

    def dout(self, level, message):
        self._logger.log(level, self._format_dout_message(message))

    def error_code(self, errorno):
        try:
            return self._error_code_map[str(errorno)]
        except KeyError:
            raise ValueError('TcServiceUtils.error_code: undefined errorno %s' % errorno)
        except Exception as e:
            raise TcdsError(str(e))

    def run_shell_command(self, command):
        pcmd = ['%s' % command]
        self.dout(logging.INFO, pcmd)
        pipe = subprocess.Popen(pcmd, stdout = subprocess.PIPE,
                stderr = subprocess.PIPE, shell = True)
        pout = pipe.communicate()
        if pipe.returncode:
            raise TcdsError(pout)

    def run_remote_command(self, remote_host, command):
        self.run_shell_command(''.join(('ssh -o UserKnownHostsFile=/dev/null -t -t ', remote_host, ' "', command, '"')))


class TcdsError(Exception):
    pass


class Tcdb(object):
    """Helper class to connect to TCDB"""
    @staticmethod
    def connect():
        exec 'import psycopg2'
        exec 'import tcloud.util.globalconfig as globalconfig'
        gbConfig = globalconfig.GlobalConfig()
        conn_string = []
        options_to_get = {
            'host': globalconfig.OPTION_DB_IP,
            'dbname': globalconfig.OPTION_DB_NAME,
            'user': globalconfig.OPTION_DB_USERNAME,
            'password': globalconfig.OPTION_DB_PASSWORD
        }
        for key, option in options_to_get.items():
            value = gbConfig.getValue(globalconfig.SECTION_DB, option)
            if value:
                conn_string.append('='.join((key, value)))
        conn_string.append('='.join(('port', '5432')))
        return psycopg2.connect(' '.join(conn_string))


class Resolv(object):
    def uuid_to_ip(self, uuid): pass


class LocalResolv(Resolv):
    mapping = None
    def __init__(self):
        self.mapping = {}

    def uuid_to_ip(self, uuid):
        if uuid in self.mapping:
            return self.mapping[uuid]
        else:
            return ''


class TcdbResolv(Resolv):
    conn = None
    class ip(object):
        @staticmethod
        def long_to_str(long_ip):
            _MAX_IP = 0xffffffff
            if _MAX_IP < long_ip or long_ip < 0:
                    raise TypeError("expected int between 0 and %d inclusive" % _MAX_IP)
            return '%d.%d.%d.%d' % (long_ip>>24 & 255, long_ip>>16 & 255, long_ip>>8 & 255, long_ip & 255)

    def __init__(self):
        self.conn = Tcdb.connect()

    def uuid_to_ip(self, uuid):
        cur = self.conn.cursor()
        cur.execute('select "IP_ADDRESS"."IP4"\
            FROM "IP_ADDRESS","NETWORK_INTERFACE","NETWORK"\
            WHERE "NETWORK_INTERFACE"."MAC_ADDRESS"="IP_ADDRESS"."MAC_ADDRESS"\
            AND "NETWORK_INTERFACE"."NETWORK_ID"="NETWORK"."ID"\
            AND "NETWORK"."FUNCTION_TYPE"=1\
            AND "NETWORK_INTERFACE"."MACHINE_ID"=%s', (uuid,)
        )
        row = cur.fetchone()
        cur.close()
        if row is None:
            return KeyError
        return self.ip.long_to_str(row[0])



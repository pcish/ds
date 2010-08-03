import subprocess

class ServiceGlobals(object):
    SUCCESS = 0
    ERROR_GENERAL = 1
    resolv = None
    CONFIG_FILE_PATH_PREFIX = ''
    def __init__(self, resolv=None):
        self.resolv = resolv
    def get_libceph(self, config_file_path): return None
    def dout(self, level, message): pass
    def error_code(self, errorno): pass
    def run_shell_command(self, command): pass
    def run_remote_command(self, remote_host, command): pass


class LocalDebugServiceGlobals(ServiceGlobals):
    def dout(self, level, message):
        print message

    def error_code(self, errorno):
        return errorno

    def run_shell_command(self, command):
        print command

    def run_remote_command(self, remote_host, command):
        print 'ssh %s %s' % (remote_host, command)

class LocalUnittestServiceGlobals(ServiceGlobals):
    shell_commands = None

    def __init__(self, resolv):
        ServiceGlobals.__init__(self, resolv)
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

class TcServiceGlobals(ServiceGlobals):
    logger = None
    CONFIG_FILE_PATH_PREFIX = '/etc/ceph'
    error_code_map = None
    def __init__(self, resolv):
        ServiceGlobals.__init__(self, resolv)
        exec 'from tcloud.util.logger import TCLog'
        self.logger = TCLog('tcdsService')
        exec 'from tcloud.util.errorcode import TC_DISTRIBUTED_STORAGE_ERROR, TC_SUCCESS'
        self.error_code_map = {}
        self.error_code_map[str(self.SUCCESS)] = TC_SUCCESS
        self.error_code_map[str(self.ERROR_GENERAL)] = TC_DISTRIBUTED_STORAGE_ERROR

    def get_libceph(self, config_file_path):
        exec 'from ceph.libceph import LibCeph'
        libceph = LibCeph(['', '-c', '%s' % config_file_path])
        return libceph

    def dout(self, level, message):
        self.logger.log(level, message)

    def error_code(self, errorno):
        try:
            return self.error_code_map[str(errorno)]
        except KeyError:
            raise ValueError('NormalServiceProfile.error_code: undefined errorno %s' % errorno)
        except Exception as e:
            raise TcdsError(str(e))

    def run_shell_command(self, command):
        pcmd = ['%s' % command]
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



class ServiceGlobals(object):
    SUCCESS = 0
    ERROR_GENERAL = 1
    resolv = None
    def __init__(self, resolv=None):
        self.resolv = resolv
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
    def __init__(self, resolv):
        ServiceGlobals.__init__(self, resolv)
        #logger = TCLog('tcdsService')

    def dout(self, level, message):
        logger.log(level, message)

    def error_code(self, errorno):
        if errorno == self.SUCCESS:
            return
        elif errorno == self.ERROR_GENERAL:
            return
        else:
            raise ValueError('NormalServiceProfile.error_code: undefined errorno %s' % errorno)

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

class TcdsResolv(Resolv):
    conn = None
    def __init__(self):
        exec 'import psycopg2'
        exec 'import tcloud.util.globalconfig as globalconfig'
        self.connect()

    def connect():
        gbConfig = globalconfig.GlobalConfig()
        conn_string = []
        options_to_get = {
            'host': globalconfig.OPTION_DB_IP,
            'dbname': globalconfig.OPTION_DB_NAME,
            'user': globalconfig.OPTION_DB_USERNAME,
            'password': globalconfig.OPTION_DB_PASSWORD
        }
        for key, option in options_to_get:
            value = gbConfig.getValue(globalconfig.SECTION_DB, option)
            if value:
                conn_string.append('='.join(key, option))
        conn_string.append('='.join('port', '5432'))
        self.conn = psycopg2.connect(' '.join(conn_string))

    def uuid_to_ip(self, uuid):
        pass
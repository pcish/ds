class ServiceGlobals(object):
    SUCCESS = 0
    ERROR_GENERAL = 1
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

class TcServiceGlobals(ServiceGlobals):
    logger = None
    def __init__(self): pass
#        logger = TCLog('tcdsService')

    def dout(self, level, message):
        logger.log(level, message)

    def error_code(self, errorno):
        if errorno == self.SUCCESS:
            return
        elif errorno == self.ERROR_GENERAL:
            return
        else:
            raise ValueError('NormalServiceProfile.error_code: undefined errorno %s' % errorno)

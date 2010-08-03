from threading import Thread

class _PyCephLibTask(Thread):
    function = None
    args = None
    return_value = None
    def __init__(self, function=None, args=None):
        Thread.__init__(self)
        self.function = function
        self.args = args

    def execute(self, timeout=10):
        self.start()
        self.join(timeout)
        if self.isAlive():
            raise LibCephException("Operation timed out")

    def run(self):
        self.return_value = self.function(*self.args)


class PyCephLibError(Exception):
    """Base class for errors in the pycephlib package."""
    def __init_(self, value):
        Exception.__init__(self, value)
        if isinstance(value, tuple):
            self.value = "\n".join(value)
        else:
            self.value = value


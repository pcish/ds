"""Python wrapper class for libceph

Exports: LibCeph
"""

from ctypes import c_int, c_bool, c_char_p, c_long, c_ulonglong, CDLL, create_string_buffer, sizeof, byref
from tcloud.ds.ceph.common import _PyCephLibTask, PyCephLibError

class LibCeph:

    """Python wrapper class for Ceph's client library libceph

    Currently implemented libceph functions:
        df
        open
        write
        fsync
        close
        unlink
    """

    _init_success = False

    def __init__(self, initargs=None, dllname='libceph.so.1'):
        """

        Known limitation: if __init__ is called with invalid ceph initargs
                          the calling process must terminate before trying
                          again with correct initargs

        @type  initargs: c_char_p list
        @param initargs: list of arguments to pass to the ceph client's
                         init. First argument should always be left blank
        @type  dllname: string
        @param dllname: name of the libceph library
        """
        self.libceph = CDLL(dllname)
        if initargs is not None:
            argv_class = c_char_p * len(initargs)
            argv = argv_class(*initargs)
            argc = len(argv)
        else:
            argv = None
            argc = 0
        if self.libceph.ceph_initialize(argc, argv) != 0:
            raise PyCephLibError("Error initializing libceph client.")
        self._init_success = True
        if self.libceph.ceph_mount() != 0:
            raise PyCephLibError("Error mounting depot with libceph.")

    def __del__(self):
        if self._init_success:
            self.libceph.ceph_deinitialize()
            self._init_success = False

    def df(self):
        """Get disk usage statistics

        @rtype:  list
        @return: list with two elements: (available bytes, total bytes)
        """
        avail = c_ulonglong(0)
        total = c_ulonglong(0)
        task = _PyCephLibTask(self.libceph.ceph_df,
                              (byref(avail), byref(total)))
        task.execute()
        return (avail.value, total.value)

    def open(self, filename, flags, mode):
        """Open a file

        @type  filename: string
        @param filename: name of the file to open
        @type  flags: int
        @param flags: see os open() flag constants
        @type  mode: int
        @param mode: see stat module

        @rtype:  int
        @return: if >= 0: a file handle representing the opened file
                 elif < 0: error code representing a failure
        """
        self.libceph.ceph_open.restype = c_int
        task = _PyCephLibTask(self.libceph.ceph_open, (filename, flags, mode))
        task.execute()
        return task.return_value

    def write(self, file_handle, string_buffer, offset):
        """Write a string to a file

        @type  file_handle: int
        @param file_handle: file handle of the file to write to
        @type  string_buffer: string
        @param string_buffer: the string to write
        @type  offset: long
        @param offset: offset in the file to start writing at

        @rtype:  int
        @return: if >= 0: number of bytes written
                 elif < 0: error code representing a failure
        """
        self.libceph.ceph_write.argtypes = [c_int, c_char_p, c_long, c_long]
        task = _PyCephLibTask(self.libceph.ceph_write)
        buf = create_string_buffer(string_buffer)
        task.args = (file_handle,\
                     buf,\
                     c_long(sizeof(buf)),\
                     c_long(offset))
        task.execute()
        return task.return_value

    def fsync(self, file_handle, sync_data_only):
        """Sync a file to disk

        @type  file_handle: int
        @param file_handle: file handle of the file to sync
        @type  sync_data_only: boolean
        @param sync_data_only: whether or not to also sync metadata

        @rtype:  int
        @return: always == 0
        """
        self.libceph.ceph_fsync.argtypes = [c_int, c_bool]
        task = _PyCephLibTask(self.libceph.ceph_fsync,
                              (file_handle, sync_data_only))
        task.execute()
        return task.return_value

    def close(self, file_handle):
        """Close a file

        @type  file_handle: int
        @param file_handle: file handle of the file to close

        @rtype:  int
        @return: always == 0
        """
        task = _PyCephLibTask(self.libceph.ceph_close, (file_handle,))
        task.execute()
        return task.return_value

    def unlink(self, filename):
        """Unlink a file

        @type  filename: string
        @param filename: name of the file to unlink

        @rtype:  int
        @return: 0 on success
        """
        task = _PyCephLibTask(self.libceph.ceph_unlink, (filename,))
        task.execute()
        return task.return_value

import inspect

class VarStore(object):
    def __init__(self): print "new varstore"
    def get_depot_id(self):
        self._virtual_function()
    def set_depot_id(self, id):
        self._virtual_function()
    def get_depot_state(self):
        self._virtual_function()
    def set_depot_state(self, state):
        self._virtual_function()
    def add_daemon(self, daemon):
        self._virtual_function()
    def remove_daemon(self, daemon):
        self._virtual_function()
    def remove_daemons(self, daemon_list):
        self._virtual_function()
    def get_daemon_list(self, type):
        self._virtual_function()
    def set_daemon_host(self, daemon, daemon_id):
        self._virtual_function()
    def get_daemon_host(self, daemon):
        self._virtual_function()
    def set_replication_factor(self, factor):
        self._virtual_function()
    def get_replication_factor(self):
        self._virtual_function()
    def host_id_to_ip(self, host_id):
        self._virtual_function()
    def set_daemon_ceph_id(self, daemon, ceph_id):
        self._virtual_function()
    def get_daemon_ceph_id(self, daemon):
        self._virtual_function()
    def _virtual_function(self):
        assert 0, 'virtual function %s called' % inspect.stack()[1][3]

class MultiVarStore(VarStore, list):
    pass

class CephConfVarStore(VarStore):
    pass

class TcdbVarStore(VarStore):
    def get_depot_id(self): assert(0)
    def set_depot_id(self, id): assert(0)
    def get_depot_state(self): assert(0)
    def set_depot_state(self, state): assert(0)
    def add_daemon(self, daemon): assert(0)
    def remove_daemon(self, daemon): assert(0)
    def remove_daemons(self, daemon_list): assert(0)
    def get_daemon_list(self, type): assert(0)
    def set_daemon_host(self, daemon, daemon_id): assert(0)
    def get_daemon_host(self, daemon): assert(0)
    def set_replication_factor(self, factor): assert(0)
    def get_replication_factor(self): assert(0)
    def host_id_to_ip(self, host_id): assert(0)
    def set_daemon_ceph_id(self, daemon, ceph_id): assert(0)
    def get_daemon_ceph_id(self, daemon): assert(0)


class _LocalVarStoreList(list):
    def __getitem__(self, key):
        if not isinstance(key, int):
            return list.__getitem__(self, self.index(key))
        else:
            return list.__getitem__(self, key)

class LocalVarStore(VarStore):
    __depot_list = None
    __daemon_list = None
    resolv = {}

    def __init__(self):
        self.__depot_list = _LocalVarStoreList()
        self.__daemon_list = _LocalVarStoreList()

    def add_depot(self, depot):
        self.__depot_list.append(depot)

    def del_depot(self, depot):
        self.__depot_list.remove(depot)

    def set_depot_id(self, depot, depot_uuid):
        self.__depot_list[depot]._localvars['uuid'] = depot_uuid

    def get_depot_id(self, depot):
        return self.__depot_list[depot]._localvars['uuid']

    def set_depot_state(self, depot, state):
        self.__depot_list[depot]._localvars['state'] = state

    def get_depot_state(self, depot):
        return self.__depot_list[depot]._localvars['state']

    def set_depot_replication_factor(self, depot, factor):
        self.__depot_list[depot]._localvars['replication_factor'] = factor

    def get_depot_replication_factor(self, depot):
        return self.__depot_list[depot]._localvars['replication_factor']

    def add_daemon(self, daemon):
        assert(self.__daemon_list.count(daemon) == 0)
        self.__daemon_list.append(daemon)

    def remove_daemon(self, daemon):
        self.__daemon_list.remove(daemon)
        assert(self.__daemon_list.count(daemon) == 0)

    def remove_daemons(self, daemon_list):
        for daemon in daemon_list:
            self.__daemon_list.remove(daemon)
            assert(self.__daemon_list.count(daemon) == 0)

    def get_depot_daemon_list(self, depot, type='all'):
        if type == 'all':
            return self.__daemon_list
        return_list = []
        for daemon in self.__daemon_list:
            if daemon.TYPE == type:
                return_list.append(daemon)
        return return_list

    def set_daemon_uuid(self, daemon, uuid):
        self.__daemon_list[daemon]._localvars['uuid'] = uuid

    def get_daemon_uuid(self, daemon):
        return self.__daemon_list[daemon]._localvars['uuid']

    def set_daemon_host(self, daemon, host_uuid):
        self.__daemon_list[daemon]._localvars['host'] = host_uuid

    def get_daemon_host(self, daemon):
        return self.__daemon_list[daemon]._localvars['host']

    def host_id_to_ip(self, host_id):
        if host_id in self.resolv:
            return self.resolv[host_id]
        else:
            return ''

    def set_daemon_ceph_name(self, daemon, ceph_name):
        self.__daemon_list[daemon]._localvars['ceph_name'] = ceph_name

    def get_daemon_ceph_name(self, daemon):
        return self.__daemon_list[daemon]._localvars['ceph_name']




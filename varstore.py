class VarStore(object):
    def get_depot_id(self): assert(0)
    def set_depot_id(self, id): assert(0)
    def get_depot_state(self): assert(0)
    def set_depot_state(self, state): assert(0)
    def add_daemon(self, daemon): assert(0)
    def remove_daemon(self, daemon): assert(0)
    def remove_daemons(self, daemon_list): assert(0)
    def get_daemon_list(self): assert(0)
    def set_daemon_host(self, daemon, daemon_id): assert(0)
    def get_daemon_host(self, daemon): assert(0)
    def set_depot_replication_factor(self, factor): assert(0)
    def get_replication_factor(self): assert(0)
    def host_id_to_ip(self, host_id): assert(0)
    def set_daemon_ceph_id(self, daemon, ceph_id): assert(0)
    def get_daemon_ceph_id(self, daemon): assert(0)

class MultiVarStore(VarStore):
    pass


class TcdbVarStore(VarStore):
    def get_depot_id(self): assert(0)
    def set_depot_id(self, id): assert(0)
    def get_depot_state(self): assert(0)
    def set_depot_state(self, state): assert(0)
    def add_daemon(self, daemon): assert(0)
    def remove_daemon(self, daemon): assert(0)
    def remove_daemons(self, daemon_list): assert(0)
    def get_daemon_list(self): assert(0)
    def set_daemon_host(self, daemon, daemon_id): assert(0)
    def get_daemon_host(self, daemon): assert(0)
    def set_depot_replication_factor(self, factor): assert(0)
    def get_replication_factor(self): assert(0)
    def host_id_to_ip(self, host_id): assert(0)
    def set_daemon_ceph_id(self, daemon, ceph_id): assert(0)
    def get_daemon_ceph_id(self, daemon): assert(0)


class _LocalVarStoreDaemonList(list):
    def __getitem__(self, key):
        if not isinstance(key, int):
            return list.__getitem__(self, self.index(key))
        else:
            return list.__getitem__(self, key)

class LocalVarStore(VarStore):
    __depot_id = None
    __depot_state = None
    __depot_replication_factor = None
    __daemon_list = _LocalVarStoreDaemonList()
    resolv = {}

    def set_depot_id(self, depot_id):
        self.__depot_id = depot_id

    def get_depot_id(self):
        return self.__depot_id

    def set_depot_state(self, state):
        self.__depot_state = state

    def get_depot_state(self):
        return self.__depot_state

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

    def get_daemon_list(self):
        return self.__daemon_list

    def set_daemon_host(self, daemon, daemon_id):
        self.__daemon_list[daemon].id = daemon_id

    def get_daemon_host(self, daemon):
        return self.__daemon_list[daemon].id

    def set_depot_replication_factor(self, factor):
        self.__depot_replication_factor = factor

    def get_replication_factor(self):
        return self.__depot_replication_factor

    def host_id_to_ip(self, host_id):
        if host_id in self.resolv:
            return self.resolv[host_id]
        else:
            return None

    def set_daemon_ceph_id(self, daemon, ceph_id):
        self.__daemon_list[daemon].ceph_id = ceph_id

    def get_daemon_ceph_id(self, daemon):
        return self.__daemon_list[daemon].ceph_id



import inspect

class VarStore(object):
    def __init__(self): print "new varstore"
    def add_depot(self, depot):
        self._virtual_function()
    def del_depot(self, depot):
        self._virtual_function()
    def set_depot_id(self, depot, depot_uuid):
        self._virtual_function()
    def get_depot_id(self, depot):
        self._virtual_function()
    def set_depot_state(self, depot, state):
        self._virtual_function()
    def get_depot_state(self, depot):
        self._virtual_function()
    def set_depot_replication_factor(self, depot, factor):
        self._virtual_function()
    def get_depot_replication_factor(self, depot):
        self._virtual_function()
    def add_daemon(self, daemon):
        self._virtual_function()
    def remove_daemon(self, daemon):
        self._virtual_function()
    def remove_daemons(self, daemon_list):
        self._virtual_function()
    def get_depot_daemon_list(self, depot, type='all'):
        self._virtual_function()
    def set_daemon_uuid(self, daemon, uuid):
        self._virtual_function()
    def get_daemon_uuid(self, daemon):
        self._virtual_function()
    def set_daemon_host(self, daemon, host_uuid):
        self._virtual_function()
    def get_daemon_host(self, daemon):
        self._virtual_function()
    def host_id_to_ip(self, host_id):
        self._virtual_function()
    def set_daemon_ceph_name(self, daemon, ceph_name):
        self._virtual_function()
    def get_daemon_ceph_name(self, daemon):
        self._virtual_function()
    def _virtual_function(self):
        assert 0, 'virtual function %s called' % inspect.stack()[1][3]

class MultiVarStore(VarStore, list):
    pass

class CephConfVarStore(VarStore):
    pass

class TcdbVarStore(VarStore):
    con = None
    def __init__(self):
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
        for key, option in options_to_get:
            value = gbConfig.getValue(globalconfig.SECTION_DB, option)
            if value:
                conn_string.append('='.join(key, option))
        conn_string.append('='.join('port', '5432'))
        self.con = psycopg2.connect(' '.join(conn_string))

    def add_depot(self, depot, uuid, replication, state):
        c = self.con.cursor()
        c.execute('insert into "TCDS_DEPOT" values (%s, %s, %s)',
            (uuid, replication, state))
        conn.commit()
        c.close()
    def del_depot(self, depot):
        self._virtual_function()
    def set_depot_id(self, depot, depot_uuid):
        self._virtual_function()
    def get_depot_id(self, depot):
        self._virtual_function()
    def set_depot_state(self, depot, state):
        self._virtual_function()
    def get_depot_state(self, depot):
        c = self.con.cursor()
        c.execute('select "STATE" from "TCDS_DEPOT" where "ID"=%s', (self.get_depot_id(depot),))
        return c.fetchone()

    def set_depot_replication_factor(self, depot, factor):
        self._virtual_function()
    def get_depot_replication_factor(self, depot):
        c = self.con.cursor()
        c.execute('select "REPLICATION" from "TCDS_DEPOT" where "ID"=%s',
            (self.get_depot_id(depot),))
        return c.fetchone()

    def add_daemon(self, daemon, uuid, host, ceph_name):
        c = self.con.cursor()
        c.execute('insert into "TCDS_NODE" values (%s, %s, %s, %s, %s)',
            (uuid, self.get_depot_id(daemon.depot), host, self._daemon_type_to_int(daemon.TYPE), ceph_name))

    def remove_daemon(self, daemon):
        self._virtual_function()

    def remove_daemons(self, daemon_list):
        for daemon in daemon_list:
            c.execute('delete from "TCDS_NODE" where "DEPOT_ID" = %s and "ID" = %s',
                (self.get_depot_id(daemon.depot), self.get_daemon_uuid(daemon)))

    def get_depot_daemon_list(self, depot, type='all'):
        self._virtual_function()

    def set_daemon_uuid(self, daemon, uuid):
        self._virtual_function()
    def get_daemon_uuid(self, daemon):
        self._virtual_function()
    def set_daemon_host(self, daemon, host_uuid):
        self._virtual_function()
    def get_daemon_host(self, daemon):
        self._virtual_function()
    def host_id_to_ip(self, host_id):
        self._virtual_function()
    def set_daemon_ceph_name(self, daemon, ceph_name):
        self._virtual_function()
    def get_daemon_ceph_name(self, daemon):
        self._virtual_function()

    def _daemon_type_to_int(self, daemon_type):
        return {'mon': 0, 'mds': 1, 'osd': 2}[daemon_type]


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

    def add_depot(self, depot, uuid, replication, state):
        self.__depot_list.append(depot)
        self.__depot_list[depot]._localvars['uuid'] = uuid
        self.__depot_list[depot]._localvars['replication_factor'] = replication
        self.__depot_list[depot]._localvars['state'] = state

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

    def add_daemon(self, daemon, uuid, host, ceph_name):
        assert(self.__daemon_list.count(daemon) == 0)
        self.__daemon_list.append(daemon)
        self.__daemon_list[daemon]._localvars['uuid'] = uuid
        self.__daemon_list[daemon]._localvars['host'] = host
        self.__daemon_list[daemon]._localvars['ceph_name'] = ceph_name

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




import inspect

class VarStore(object):
    PERSISTENT = False
    def add_depot(self, depot):
        self._virtual_function()

    def del_depot(self, depot):
        self._virtual_function()

    def set_depot_uuid(self, depot, depot_uuid):
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

    def set_daemon_host(self, daemon, host_uuid):
        self._virtual_function()

    def get_daemon_host(self, daemon):
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
    PERSISTENT = True
    conn = None
    def __init__(self):
        exec 'from serviceglobals import Tcdb'
        self.conn = Tcdb.connect()

    def add_depot(self, depot, uuid, replication, state):
        cur = self.conn.cursor()
        cur.execute('insert into "TCDS_DEPOT" values (%s, %s, %s)',
            (uuid, replication, state))
        self.conn.commit()
        cur.close()

    def del_depot(self, depot):
        cur = self.conn.cursor()
        cur.execute('delete from "TCDS_DEPOT" where "ID"=%s', (depot.uuid,))
        self.conn.commit()
        cur.close()

    def set_depot_uuid(self, depot, depot_uuid):
        cur = self.conn.cursor()
        cur.execute('update "TCDS_DEPOT" set "ID"=%s where "ID"=%s',
            (depot_uuid, depot.uuid))
        self.conn.commit()
        cur.close()

    def set_depot_state(self, depot, state):
        cur = self.conn.cursor()
        cur.execute('update "TCDS_DEPOT" set "STATE"=%s where "ID"=%s',
            (state, depot.uuid))
        self.conn.commit()
        cur.close()

    def get_depot_state(self, depot):
        cur = self.conn.cursor()
        cur.execute('select "STATE" from "TCDS_DEPOT" where "ID"=%s', (depot.uuid,))
        row = cur.fetchone()
        cur.close()
        if row is None:
            raise KeyError
        return int(row[0])

    def set_depot_replication_factor(self, depot, factor):
        cur = self.conn.cursor()
        cur.execute('update "TCDS_DEPOT" set "REPLICATION"=%s where "ID"=%s',
            (factor, depot.uuid))
        conn.commit()
        cur.close()

    def get_depot_replication_factor(self, depot):
        cur = self.conn.cursor()
        cur.execute('select "REPLICATION" from "TCDS_DEPOT" where "ID"=%s',
            (depot.uuid,))
        row = cur.fetchone()
        cur.close()
        if row is None:
            raise KeyError
        return int(row[0])

    def get_depot_list(self):
        cur = self.conn.cursor()
        cur.execute('select "ID", "REPLICATION", "STATE" from "TCDS_DEPOT"')
        rows = cur.fetchall()
        cur.close()
        ret_list = []
        for depot in rows:
            ret_list.append({'uuid': depot[0], 'replication_factor': depot[1], 'state': depot[2]})
        return ret_list

    def add_daemon(self, daemon, uuid, host, ceph_name):
        cur = self.conn.cursor()
        cur.execute('insert into "TCDS_NODE" values (%s, %s, %s, %s, %s)',
            (uuid, daemon.depot.uuid, host, self._daemon_type_to_int(daemon.TYPE), ceph_name))
        self.conn.commit()
        cur.close()

    def remove_daemon(self, daemon):
        cur = self.conn.cursor()
        cur.execute('delete from "TCDS_NODE" where "DEPOT_ID"=%s and "ID"=%s',
            (daemon.depot.uuid, daemon.uuid))
        self.conn.commit()
        cur.close()

    def remove_daemons(self, daemon_list):
        cur = self.conn.cursor()
        for daemon in daemon_list:
            cur.execute('delete from "TCDS_NODE" where "DEPOT_ID"=%s and "ID"=%s',
                (daemon.depot.uuid, daemon.uuid))
        self.conn.commit()
        cur.close()

    def get_depot_daemon_list(self, depot, type='all'):
        cur = self.conn.cursor()
        cur.execute('select "ID", "SERVER_ID", "CEPH_ID", "ROLE" from "TCDS_NODE" where "DEPOT_ID"=%s',
            (depot.uuid,))
        rows = cur.fetchall()
        cur.close()
        ret_list = []
        for daemon in rows:
            if type == 'all' or _daemon_int_to_type(daemon[3]) == type:
                ret_list.append({'uuid': daemon[0], 'type': self._daemon_int_to_type(daemon[3]), 'host': daemon[1], 'ceph_name': str(daemon[2])})
        return ret_list

    def set_daemon_uuid(self, daemon, uuid):
        cur = self.conn.cursor()
        cur.execute('update "TCDS_NODE" set "ID"=%s where "ID"=%s',
            (host_uuid, daemon.uuid))
        self.conn.commit()
        cur.close()

    def set_daemon_host(self, daemon, host_uuid):
        cur = self.conn.cursor()
        cur.execute('update "TCDS_NODE" set "SERVER_ID"=%s where "ID"=%s',
            (host_uuid, daemon.uuid))
        self.conn.commit()
        cur.close()

    def get_daemon_host(self, daemon):
        cur = self.conn.cursor()
        cur.execute('select "SERVER_ID" from "TCDS_NODE" where "ID"=%s',
            (daemon.uuid,))
        row = cur.fetchone()
        cur.close()
        if row is None:
            raise KeyError
        return row[0]

    def set_daemon_ceph_name(self, daemon, ceph_name):
        cur = self.conn.cursor()
        cur.execute('update "TCDS_NODE" set "CEPH_ID"=%s where "ID"=%s',
            (ceph_name, daemon.uuid))
        self.conn.commit()
        cur.close()

    def get_daemon_ceph_name(self, daemon):
        cur = self.conn.cursor()
        cur.execute('select "CEPH_ID" from "TCDS_NODE" where "ID"=%s',
            (daemon.uuid,))
        row = cur.fetchone()
        cur.close()
        if row is None:
            raise KeyError
        return str(row[0])

    def _daemon_type_to_int(self, daemon_type):
        return {'mon': 0, 'mds': 1, 'osd': 2}[daemon_type]

    def _daemon_int_to_type(self, type_int):
        return {'0': 'mon', '1': 'mds', '2': 'osd'}[str(type_int)]


class _LocalVarStoreList(list):
    def __getitem__(self, key):
        if not isinstance(key, int):
            return list.__getitem__(self, self.index(key))
        else:
            return list.__getitem__(self, key)

class LocalVarStore(VarStore):
    depots = None
    class InternalObject(object):
        _localvars = {}
        _children = {}
        def __init__(self):
            self._localvars = {}
            self._children = {}

    def __init__(self):
        self.depots = {}

    def add_depot(self, depot, uuid, replication, state):
        self.depots[uuid] = self.InternalObject()
        self.depots[uuid]._localvars['replication'] = replication
        self.depots[uuid]._localvars['state'] = state

    def del_depot(self, depot):
        del self.depots[depot.uuid]

    def set_depot_uuid(self, depot, depot_uuid):
        self.depots[depot_uuid] = self.depots[depot.uuid]
        del self.depots[depot.uuid]

    def set_depot_state(self, depot, state):
        self.depots[depot.uuid]._localvars['state'] = state

    def get_depot_state(self, depot):
        return self.depots[depot.uuid]._localvars['state']

    def set_depot_replication_factor(self, depot, factor):
        self.depots[depot.uuid]._localvars['replication'] = factor

    def get_depot_replication_factor(self, depot):
        return self.depots[depot.uuid]._localvars['replication']

    def get_depot_list(self):
        ret_list = []
        for uuid, depot in self.depots[depot.uuid].items():
            ret_list.append({'uuid': uuid, 'replication_factor': depot._localvars['replication'], 'state': depot._localvars['state']})
        return ret_list

    def add_daemon(self, daemon, uuid, host, ceph_name):
        self.depots[daemon.depot.uuid]._children[uuid] = self.InternalObject()
        self.depots[daemon.depot.uuid]._children[uuid]._localvars['host'] = host
        self.depots[daemon.depot.uuid]._children[uuid]._localvars['ceph_name'] = str(ceph_name)
        self.depots[daemon.depot.uuid]._children[uuid]._localvars['type'] = daemon.TYPE

    def remove_daemon(self, daemon):
        del self.depots[daemon.depot.uuid]._children[daemon.uuid]

    def remove_daemons(self, daemon_list):
        for daemon in daemon_list:
            del self.depots[daemon.depot.uuid]._children[daemon.uuid]

    def get_depot_daemon_list(self, depot, type='all'):
        ret_list = []
        for uuid, daemon in self.depots[depot.uuid]._children.items():
            if type == 'all' or daemon._localvars['type'] == type:
                ret_list.append({
                    'uuid': uuid, 'type': daemon._localvars['type'],
                    'host': daemon._localvars['host'],
                    'ceph_name': daemon._localvars['ceph_name']
                })
        return ret_list

    def set_daemon_uuid(self, daemon, uuid):
        self.depots[daemon.depot.uuid]._children[uuid] = self.depots[daemon.depot.uuid]._children[daemon.uuid]
        del self.depots[daemon.depot.uuid]._children[daemon.uuid]

    def set_daemon_host(self, daemon, host_uuid):
        self.depots[daemon.depot.uuid]._children[daemon.uuid]._localvars['host'] = host_uuid

    def get_daemon_host(self, daemon):
        return self.depots[daemon.depot.uuid]._children[daemon.uuid]._localvars['host']

    def set_daemon_ceph_name(self, daemon, ceph_name):
        self.depots[daemon.depot.uuid]._children[daemon.uuid]._localvars['ceph_name'] = str(ceph_name)

    def get_daemon_ceph_name(self, daemon):
        return self.depots[daemon.depot.uuid]._children[daemon.uuid]._localvars['ceph_name']


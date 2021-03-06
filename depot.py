"""Classes pertaining to the control of a depot

Exports:
* Depot
* (Depot.DepotInfo)
"""
import logging
import os
import copy
import stat

from cephconf import TCCephConf
from daemon import Mon, Mds, Osd
from tcdsutils import TcdsError

class OsdGroup(list):
    TYPE = ''
    seqno = None

class Depot(object):
    """Controls and maintains the state of a depot

    Dev note: Logic, statements, operations and commands that affect the entire
        cluster belong in this class. If it affects only a single daemon, go to
        the Daemon class. If it affects more than one depot, go to the
        TcdsService class.
    """
    uuid = None
    _daemon_map = None
    service = None
    utils = None
    var = None
    CONSTANTS = {
        'STATE_OFFLINE': 0,
        'STATE_ONLINE': 1
    }
    config_file_path = None
    config = None

    class DepotInfo(dict):
        """Class for retrieving information about a depot

        Usage: call populate() after creating an instance, passing it a depot

        Keys:                 Type:    Values:
        'depot_id'          : string - uuid of the depot
        'depot_replication' : int    - replication factor of the depot
        'depot_state'       : string - state of the depot
        'depot_capacity'    : string - capacity of the depot in bytes
        'depot_usage'       : string - used space of the depot in bytes
        """
        def __init__(self):
            dict.__init__(self)

        @staticmethod
        def get_state_string(state):
            """Maps internal depot state representation to strings

            @type  state  depot internal state reprentation
                          (int at the time of writing)
            @param state  depot state

            @rtype  string
            @return human readable string of the depot state
            """
            if state == Depot.CONSTANTS['STATE_OFFLINE']:
                return 'not ready'
            elif state == Depot.CONSTANTS['STATE_ONLINE']:
                return 'ready'

        def populate(self, depot):
            """Fills in predefined fields with information from the given
            depot"""
            self['depot_id'] = depot.uuid
            self['depot_replication'] = depot.var.get_depot_replication_factor(depot)
            self['depot_state'] = self.get_state_string(depot.var.get_depot_state(depot))
            if self['depot_state'] == 'ready':
                libceph = depot.utils.get_libceph(depot.config_file_path)
                if libceph is not None:
                    (avail, total) = libceph.df()
                    self['depot_capacity'] = str(total)
                    self['depot_usage'] = str(total - avail)
                    return
                else:
                    depot.utils.dout(logging.INFO, 'returning fake info because get_libceph failed')
            else:
                depot.utils.dout(logging.INFO, 'returning fake info because depot is not ready')
            self['depot_capacity'] = 0
            self['depot_usage'] = 0

    def __init__(self, service, uuid):
        self.service = service
        self.uuid = uuid
        self._daemon_map = {}
        self.utils = service.utils
        self.var = service.var
        self.config_file_path = os.path.join(self.utils.CONFIG_FILE_PATH_PREFIX, '%s.conf' % self.uuid)

    def _load_saved_state(self):
        """Read and reproduce the state of the cluster from this instance's
        varstore."""
        daemon_spec_list = self.var.get_depot_daemon_list(self)
        for daemon_spec in daemon_spec_list:
            daemon = self._new_daemon_instance(daemon_spec['type'], daemon_spec['uuid'])
            self._daemon_map[daemon.uuid] = daemon
            daemon._load_saved_state()
        self._generate_ceph_conf()

    def setup(self):
        """Prepares the depot for operation.

        Call after __init__
        """
        self._generate_ceph_conf()

    def _generate_ceph_conf(self):
        """Creates from scratch a ceph.conf that matches the current state of
        the depot."""
        self.config = TCCephConf()
        self.config.create_default(self.uuid)
        daemon_list = self.get_daemon_list()
        for daemon in daemon_list:
            daemon.add_to_config(self.config)

    def get_info(self):
        """Return a DepotInfo instance with this depot's information"""
        depot_info = self.DepotInfo()
        depot_info.populate(self)
        return depot_info

    def _new_daemon_instance(self, daemon_type, uuid):
        """Create and return a new daemon instance

        @type  daemon_type  string
        @param daemon_type  the type of daemon to create. Valid types at the
                            time of writing include 'mon','mds','osd'
        @type  uuid  string
        @param uuid  uuid of the new daemon

        @rtype  instance of a Daemon subclass
        @return the newly created Daemon
        """
        if daemon_type == 'mon':
            return Mon(self, uuid)
        elif daemon_type == 'mds':
            return Mds(self, uuid)
        elif daemon_type == 'osd':
            return Osd(self, uuid)
        else:
            raise ValueError('storage_roles should be one of mon, mds, osd')

    def add_daemons(self, daemon_spec_list):
        """Create daemons and add daemons to the depot

        Creates, registers, sets up, and if conditions allow, starts the given
        daemons under the depot.

        @type  daemon_spec_list list
        @param daemon_spec_list list of daemon specification dictionaries. Each
                    daemon specification should contain the fields 'type',
                    'host' and 'uuid'; where 'type' is a valid daemon type,
                    'host is the uuid of the daemon's host machine, and 'uuid'
                    is the uuid string of the new daemon

        @rtype  list
        @return a list of the daemons instances created
        """
        orig_num_osd = self._get_daemon_count()['num_osd']
        new_daemon_list = []
        new_mons = []
        for daemon_spec in daemon_spec_list:
            # Create
            new_daemon = self._new_daemon_instance(daemon_spec['type'],
                                                   daemon_spec['uuid'])
            new_daemon_list.append(new_daemon)
            if new_daemon.TYPE == 'mon':
                new_mons.append(new_daemon)
            new_ceph_name = self._get_next_ceph_name_for(new_daemon.TYPE)
            # Register
            self.var.add_daemon(new_daemon, daemon_spec['uuid'],
                                daemon_spec['host'], new_ceph_name)
            self._daemon_map[daemon_spec['uuid']] = new_daemon

        if self.get_state() == self.CONSTANTS['STATE_OFFLINE']:
            daemon_count = self._get_daemon_count()
            if Depot._get_meets_min_requirements(replication=self.var.get_depot_replication_factor(self), **daemon_count):
                self.activate()
            else:
                self.utils.dout(logging.DEBUG, 'Add complete, not activating (daemon count=%s, replication=%s)' % (daemon_count, self.var.get_depot_replication_factor(self)))
        elif self.get_state() == self.CONSTANTS['STATE_ONLINE']:
            if self._check_depot() is not True:
                raise TcdsError('aborting add because depot does not appear to be usable')
            # Setup
            old_config = copy.deepcopy(self.config)
            if len(new_mons) > 0:
                for daemon in new_mons:
                    cmd = 'ceph -c %s mon add %s %s:6789' % (
                        self.config_file_path, daemon.get_ceph_name(),
                        daemon.get_host_ip())
                    self.utils.run_shell_command(cmd)
                    self._wait_for_map_update()
                    """NOTE: Something bad might happen if the number of
                    monitors being added exceeds the number of original
                    monitors. E.g. if there are three monitors and four more
                    are added, when adding the fourth the monmap proposal should
                    fail because only three out of six monitors are active."""

            for daemon in new_daemon_list:
                daemon.setup(old_config)
            for daemon in new_daemon_list:
                daemon.add_to_config(self.config)
            for daemon in new_daemon_list:
                daemon.set_config(self.config)

            num_osd = self._get_daemon_count()['num_osd']
            if num_osd > orig_num_osd:
                cmd = 'ceph -c %s osd setmaxosd %s' % (self.config_file_path, num_osd)
                self.utils.run_shell_command(cmd)
                new_crushmap = self._generate_crushmap()
                cmd = 'ceph -c %s osd setcrushmap -i %s' % (self.config_file_path, new_crushmap)
                self.utils.run_shell_command(cmd)

            # Start
            for daemon in new_daemon_list:
                daemon.activate()
                if self._check_depot() is not True:
                    self.utils.dout(logging.WARNING, 'check depot failed during add, hopefully this does not matter...')
            self.write_config()
        return new_daemon_list

    def _wait_for_map_update(self):
        """Returns only after the monitors have updated their maps"""
        exec 'import time'
        time.sleep(5)   # FIXME: ideas welcome...

    def _generate_crushmap(self):
        """Returns the path to a crushmap that matches the current state

        @rtype  string
        @return path to an encoded crushmap ready for injection
        """
        num_osd = self._get_daemon_count()['num_osd']
        cmd = 'osdmaptool --createsimple %s --clobber /tmp/osdmap.junk --export-crush /tmp/crush.new' % (num_osd,)
        self.utils.run_shell_command(cmd)
        return '/tmp/crush.new'

    def remove_nodes(self, node_list, force=False):
        """Removes all daemons whose host is one of the hosts given

        By default, this operation will check that after performing the
        removal, the depot will remain healty (as per the definition in
        Depot._get_meets_min_requirements()). If the check fails, the
        opertaion will be aborted.

        @type  node_list  list
        @param node_list  list of host machine uuid strings on which to remove
                          daemons
        @type  force      boolean
        @param force      if True, does not perform checks and will always
                          attempt to remove the target daemons

        @rtype  list
        @return a list containing the daemon instances removed
        """
        daemon_list = self.get_daemon_list()
        daemon_count = self._get_daemon_count()
        remove_pending = []
        processed_nodes = []
        for node_id in node_list:
            if node_id in processed_nodes: # skip duplicates
                continue
            processed_nodes.append(node_id)
            for daemon in daemon_list:
                if daemon.get_host_id() == node_id:
                    remove_pending.append(daemon)
                    daemon_count['num_'+daemon.TYPE] = daemon_count['num_'+daemon.TYPE] - 1

        if force or self._get_meets_min_requirements(replication=self.var.get_depot_replication_factor(self), **daemon_count):
            self._del_daemons(remove_pending)
            return remove_pending
        else:
            raise TcdsError('remove_nodes: remove aborted because depot daemon count will fall below min requirements')

    def _del_daemons(self, daemons_to_remove):
        """Deletes and unregisters the given daemons

        @type  daemons_to_remove    list
        @param daemons_to_remove    list of daemon instances to remove
        """
        for daemon in daemons_to_remove:
            daemon.delete()
            daemon.del_from_config(self.config)
            del self._daemon_map[daemon.uuid]
        self.var.remove_daemons(daemons_to_remove)
        self.write_config()

    def get_state(self):
        """Return the value of the state of the depot"""
        return self.var.get_depot_state(self)

    def set_state(self, state):
        """Set the value of the state of the depot"""
        self.var.set_depot_state(self, state)

    def get_daemon_list(self, daemon_type='all'):
        """Return a list of all daemons whose type is daemon_type

        @type  daemon_type  string
        @param daemon_type  the type of the daemons to return or 'all'

        @rtype  list
        @return list of all daemons in the depot matching daemon_type
        """
        if daemon_type == 'all':
            return self._daemon_map.values()
        return_list = []
        for daemon in self._daemon_map.values():
            if daemon.TYPE == daemon_type:
                return_list.append(daemon)
        return return_list

    def _get_daemon_count(self):
        """Count the number of daemons (by type)

        @rtype  dict
        @return dict with keys num_<type> and values being the count
        """
        daemon_list = self.get_daemon_list()
        daemon_count = {'num_mon': 0, 'num_mds':0, 'num_osd':0}
        for daemon in daemon_list:
            daemon_count['num_'+daemon.TYPE] = daemon_count['num_'+daemon.TYPE] + 1
        return daemon_count

    def apply_replication_factor(self, factor):
        """Apply the replication factor setting to the depot

        @type  factor   int
        @param factor   the replication factor to apply
        """
        assert self.config_file_path is not None, 'config_file_path not set'
        cmd = "ceph -c %s osd pool set metadata size %s" % (self.config_file_path, factor)
        self.utils.run_shell_command(cmd)

        cmd = "ceph -c %s osd pool set data size %s" % (self.config_file_path, factor)
        self.utils.run_shell_command(cmd)

    @staticmethod
    def _get_meets_min_requirements(replication, num_mon, num_mds, num_osd):
        """Return whether the given number of daemons can form a healthy
        cluster

        @type  replication  int
        @param replication  replication factor of the depot to form
        @type  num_mon      int
        @param num_mon      number of monitors in the depot to form
        @type  num_mds      int
        @param num_mds      number of mds in the depot to form
        @type  num_osd      int
        @param num_osd      number of osd in the depot to form

        @rtype  boolean
        @return True if a healthy depot can be formed
        """
        if type(replication) is not int \
            or type(num_mon) is not int \
            or type(num_mds) is not int \
            or type(num_osd) is not int:
            raise TypeError('int arguments expected, got %s, %s, %s, %s' %
                (type(replication), type(num_mon), type(num_mds), type(num_osd)))
        if replication < 1 or num_mon < 0 or num_mds < 0 or num_osd < 0:
            raise ValueError('got (rep,mon,mds,osd)=(%s,%s,%s,%s)' % (replication, num_mon, num_mds, num_osd))
        if num_mon >= 3 and num_mds >= 2 and num_osd >= replication:
            return True
        else:
            return False

    def _get_next_ceph_name_for(self, daemon_type):
        """Return the next ceph name that should be assigned to a daemon of the
        given type

        @type  daemon_type  string
        @param daemon_type  the daemon type for which to fetch the next name

        @rtype  string
        @return the name that should be assigned to the daemon
        """
        daemon_list = self.get_daemon_list(daemon_type)
        in_use_names = []
        for daemon in daemon_list:
            daemon_name = daemon.get_ceph_name()
            if daemon_name.isdigit():
                in_use_names.append(daemon_name)
        in_use_names.sort()
        for i in range(len(in_use_names)):
            if in_use_names[i].isdigit() and int(in_use_names[i]) != i:
                return i
        return len(in_use_names)

    def _check_ceph_ids_are_consecutive(self):
        """Return whether the ceph names currently in use are consecutive

        Consecutivity is checked individually for each daemon type and the
        function will only evaluate to True if all daemon types are
        consecutive. However, names that cannot be interpreted as int (e.g. 'a'
        ) are ignored and do not affect consecutivity.

        @rtype  boolean
        @return True if consecutive
        """
        daemon_list = self.get_daemon_list()
        daemon_list.sort(Mon.cmp_name)
        next_id = {'mon': 0, 'mds': 0, 'osd': 0}
        for daemon in daemon_list:
            daemon_name = daemon.get_ceph_name()
            if not daemon_name.isdigit():
                continue
            if next_id[daemon.TYPE] == int(daemon_name):
                next_id[daemon.TYPE] = next_id[daemon.TYPE] + 1
            else:
                return False
        return True

    def write_config(self):
        """Write the current config to disk"""
        assert self.config_file_path is not None, 'config_file_path not set'
        with open(self.config_file_path, 'wb') as config_file:
            self.config.write(config_file)

    def update_daemon_configs(self, daemon_list=None):
        """Update all daemons (or the given daemons) with the current config
        and on each daemon's host, write the config to disk."""
        if daemon_list == None:
            daemon_list = self.get_daemon_list()
        for daemon in daemon_list:
            daemon.set_config(self.config)
            daemon.write_config()

    def activate(self, first=True):
        """Activate (start) the depot (for the first time)

        This function will activate the depot by calling activate() of each
        daemon and taking necessary pre-activate and post-activate actions.
        If the depot is being activated for the first time, pass first=True
        (which is the default). This function will abort if the depot state
        is not equal to STATE_OFFLINE.

        NB: If this function is called with first=True, the depot will be
        REFORMED and all data will be LOST.

        @type  first    boolean
        @param first    pass True if this is the first time the depot is being
                        activated. Or if you want to reformat...

        @rtype  boolean
        @return True if the operation completed successfully
        """
        if self.get_state() != self.CONSTANTS['STATE_OFFLINE']:
            return False
        if first:
            daemon_list = self.get_daemon_list()
            next_id = {'mon': 0, 'mds': 0, 'osd': 0}
            for daemon in daemon_list:
                daemon.set_ceph_name(next_id[daemon.TYPE])
                daemon.add_to_config(self.config)
                next_id[daemon.TYPE] = next_id[daemon.TYPE] + 1

        self.write_config()
        for daemon in daemon_list:
            daemon.set_config(self.config)

        if first:
            cmd = "mkcephfs -c %s --allhosts" % (self.config_file_path, )
            self.utils.run_shell_command(cmd)

        for daemon in daemon_list:
            daemon.activate()

        if first:
            self.apply_replication_factor(self.var.get_depot_replication_factor(self))
        self.set_state(self.CONSTANTS['STATE_ONLINE'])
        return True

    def deactivate(self):
        """Deactivate (stop) the depot and all its daemons"""
        for daemon in self.get_daemon_list():
            daemon.deactivate()
        self.set_state(self.CONSTANTS['STATE_OFFLINE'])

    def delete(self):
        """Delete the depot and all its daemons"""
        self._del_daemons(self.get_daemon_list())
        os.remove(self.config_file_path)

    def _check_depot(self):
        """Returns whether the depot is service

        The check is performed by attempting to write a file to the filesystem

        @rtype  boolean
        @return True if the depot is in service
        """
        libceph = self.utils.get_libceph(self.config_file_path)
        retries = 3
        if libceph is None:
            return True
        testfile_name = "tcds.api.testfile"
        for _ in range(retries):
            fd = libceph.open(testfile_name, os.O_CREAT|os.O_RDWR, stat.S_IRWXU)
            if fd >= 0:
                break
        else:
            raise TcdsError('unable to create testfile')

        if fd >= 0:
            if libceph.write(fd, testfile_name, 0) < 0:
                raise TcdsError('write failed')
            libceph.fsync(fd, False)
            libceph.close(fd)
            if libceph.unlink(testfile_name) != 0:
                raise TcdsError('unlink failed')
        return True


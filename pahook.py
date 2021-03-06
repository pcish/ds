"""TCDS API
Exports the functions:
    createDepot
    deleteDepot
    getDepotInfo
    getDepotInfoList
    addStorageNodes
    removeStorageNodes

The API functions are basically wrappers for their companion functions in
TcdsService. An instance of the TcdsService class is maintained for the
functions to call.
"""
import uuid
import logging
from service import TcdsService
from varstore import TcdbVarStore as VariableStore
from tcdsutils import TcServiceUtils as Utils
from tcdsutils import TcdbResolv as Resolv

_utils = Utils(Resolv())
_service = TcdsService(_utils, VariableStore())

class TcdsApiErrorResponse(dict):
    def __init__(self, dout, code, message):
        dict.__init__(self)
        self['result_code'] = _service.utils.error_code(code)
        self['error_message'] = '%s' % message
        dout(logging.WARNING, str(self), 1)

class TcdsApiSuccessResponse(dict):
    def __init__(self, dout, additional_fields=None):
        dict.__init__(self)
        self['result_code'] = _service.utils.error_code(_service.utils.SUCCESS)
        if additional_fields is not None:
            self.update(additional_fields)
        dout(logging.INFO, str(self), 1)

def createDepot(args):
    """
    INPUT
        * replication_number (optional)

    OUTPUT
        * result_code
        * error_message on failure
        * depot_id when succeed
    """
    _utils.dout(logging.DEBUG, 'createDepot: called with %s' % args)
    NewDepotID = str(uuid.uuid4())

    if 'replication_number' in args and args['replication_number'] > 0:
        replication_number = args['replication_number']
    else:
        replication_number = 3  # default replication number

    try:
        depot = _service.create_depot(NewDepotID, replication_number)
    except Exception as e:
        return TcdsApiErrorResponse(_utils.dout, _utils.ERROR_GENERAL, e)
    else:
        if depot is not None:
            return TcdsApiSuccessResponse(_utils.dout, {'depot_id': NewDepotID})
        else:
            return TcdsApiErrorResponse(_utils.dout, _utils.ERROR_GENERAL, 'failed to create depot')

def deleteDepot(args):
    """Takes a depot uuid and deletes the corresponding depot

    @type args:     dict
    @param args:    'depot_id': uuid string of the depot to delete
    """
    _utils.dout(logging.DEBUG, 'deleteDepot: called with %s' % args)
    try:
        _service.remove_depot(args['depot_id'])
    except KeyError:
        return TcdsApiErrorResponse(_utils.dout, _utils.ERROR_GENERAL, 'No such depot.')
    except Exception as e:
        return TcdsApiErrorResponse(_utils.dout, _utils.ERROR_GENERAL, e)
    else:
        return TcdsApiSuccessResponse(_utils.dout)

def getDepotInfoList(args):
    """ INPUT
            {}

        OUTPUT
            depot_info_list
    """
    _utils.dout(logging.DEBUG, 'getDepotInfoList: called with %s' % args)
    depot_info_list = []
    for depot_id in _service._depot_map.keys():
        try:
            depot_info = dict(_service.query_depot(depot_id))
        except KeyError:
            _service.utils.dout(logging.WARNING, 'pahook.getDepotInfoList: could not query depot %s' % args)
        except Exception as e:
            return TcdsApiErrorResponse(_utils.dout, _utils.ERROR_GENERAL, e)
        else:
            depot_info_list.append(depot_info)
    return TcdsApiSuccessResponse(_utils.dout, {'depot_info_list': depot_info_list})

def getDepotInfo(args):
    """
        INPUT
            * args['depot_id']: Depot uuid

        OUTPUT
            * result['result_code']
            * result['depot_info']
            * result['depot_info']
    """
    _utils.dout(logging.DEBUG, 'getDepotInfo: called with %s' % args)
    try:
        depot_info = dict(_service.query_depot(args['depot_id']))
    except KeyError:
        return TcdsApiErrorResponse(_utils.dout, _utils.ERROR_GENERAL, 'No such depot.')
    except Exception as e:
        return TcdsApiErrorResponse(_utils.dout, _utils.ERROR_GENERAL, e)
    else:
        return TcdsApiSuccessResponse(_utils.dout, {'depot_info': depot_info})

def addStorageNodes(args):
    """
        INPUT
            * depot_id
            * node_spec_list
                * node_id
                * storage_rules
        OUTPUT
            * result_code
            * error_message on failure
    """
    _utils.dout(logging.DEBUG, 'addStorageNodes: called with %s' % args)
    try:
        daemon_spec_list = []
        for node in args['node_spec_list']:
            for role in node['storage_roles']:
                daemon_spec_list.append({'type': role, 'host': node['node_id'], 'uuid': str(uuid.uuid4())})
        addedd_daemons_uuids = _service.add_daemons_to_depot(args['depot_id'], daemon_spec_list)
    except KeyError, e:
        return TcdsApiErrorResponse(_utils.dout, _utils.ERROR_GENERAL, 'No such depot. %s' % str(e))
    except Exception as e:
        return TcdsApiErrorResponse(_utils.dout, _utils.ERROR_GENERAL, e)
    else:
        return TcdsApiSuccessResponse(_utils.dout, {'daemons_added': addedd_daemons_uuids})

def removeStorageNodes(args):
    """
    INPUT example:
    {
    "depot_id": "012b594e-ce38-4d74-b5e7-1458ba38fefb",
    "node_list":
    [
        "e4d6f138-832f-4b42-b6d4-00f8a0aef510",
        "570a67bf-c6f9-4232-8356-2f74c8d9e479",
        "d2c5d946-b888-40b3-aac2-adda05477a81"
    ]
    }
    """
    _utils.dout(logging.DEBUG, 'removeStorageNodes: called with %s' % args)
    if 'force' not in args:
        args['force'] = False
    try:
        removed_daemons_uuids = _service.del_nodes_from_depot(args['depot_id'], args['node_list'], args['force'])
    except KeyError:
        return TcdsApiErrorResponse(_utils.dout, _utils.ERROR_GENERAL, 'No such depot.')
    except Exception as e:
        return TcdsApiErrorResponse(_utils.dout, _utils.ERROR_GENERAL, e)
    else:
        return TcdsApiSuccessResponse(_utils.dout, {'daemons_removed': removed_daemons_uuids})

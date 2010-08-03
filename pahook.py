"""TCDS API
Exports the functions:
    createDepot
    getDepotInfo
    addStorageNodes
    removeStorageNodes

The API functions are basically wrappers for their companion functions in
TcdsService. An instance of the TcdsService class is maintained for the
functions to call.
"""
import uuid
from service import TcdsService
from depot import Depot
from varstore import TcdbVarStore as VariableStore
from serviceglobals import TcServiceUtils as Globals
from serviceglobals import TcdbResolv as Resolv

_service = TcdsService(Globals(Resolv()), VariableStore())

class TcdsApiErrorResponse(dict):
    def __init__(self, code, message):
        self['result_code'] = _service.utils.error_code(code)
        self['error_message'] = '%s' % message

class TcdsApiSuccessResponse(dict):
    def __init__(self, additional_fields):
        self['result_code'] = _service.utils.error_code(Globals.SUCCESS)
        self.update(additional_fields)

def createDepot(args):
    """
    INPUT
        * replication_number

    OUTPUT
        * result_code
        * error_message on failure
        * depot_id when succeed
    """

    NewDepotID = str(uuid.uuid4())

    if 'replication_number' in args and args['replication_number'] > 0:
        replication_number = args['replication_number']
    else:
        replication_number = 3  # default replication number

    try:
        depot = _service.create_depot(NewDepotID, replication_number)
    except Exception as e:
        return TcdsApiErrorResponse(Globals.ERROR_GENERAL, e)
    else:
        if depot is not None:
            return TcdsApiSuccessResponse({'depot_id': NewDepotID})
        else:
            return TcdsApiErrorResponse(Globals.ERROR_GENERAL, 'failed to create depot')

def deleteDepot(args):
    """Takes a depot uuid and deletes the corresponding depot

    @type args:     dict
    @param args:    'depot_id': uuid string of the depot to delete
    """
    try:
        _service.remove_depot(args['depot_id'])
    except KeyError:
        return TcdsApiErrorResponse(Globals.ERROR_GENERAL, 'No such depot.')
    except Exception as e:
        return TcdsApiErrorResponse(Globals.ERROR_GENERAL, e)
    else:
        return TcdsApiSuccessResponse()

def getDepotInfoList(args):
    """ INPUT
            {}

        OUTPUT
            depot_info_list
    """
    depot_info_list = []
    for depot_id in _service._depot_map.keys():
        try:
            depot_info = _service.query_depot(args['depot_id'])
        except KeyError:
            _service.utils.dout(logging.WARNING, 'pahook.getDepotInfoList: could not query depot %s' % args)
        except Exception as e:
            return TcdsApiErrorResponse(Globals.ERROR_GENERAL, e)
        else:
            depot_info_list.append(depot_info['depot_info'])
    return TcdsApiSuccessResponse({'depot_info_list': depot_info_list})

def getDepotInfo(args):
    """
        INPUT
            * args['depot_id']: Depot uuid

        OUTPUT
            * result['result_code']
            * result['depot_info']
            * result['depot_info']
    """

    try:
        depot_info = _service.query_depot(args['depot_id'])
    except KeyError:
        return TcdsApiErrorResponse(Globals.ERROR_GENERAL, 'No such depot.')
    except Exception as e:
        return TcdsApiErrorResponse(Globals.ERROR_GENERAL, e)
    else:
        return TcdsApiSuccessResponse({'depot_info': depot_info})

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
    try:
        daemon_spec_list = []
        for node in args['node_spec_list']:
            for role in node['storage_roles']:
                daemon_spec_list.append({'type': role, 'host': node['node_id'], 'uuid': str(uuid.uuid4())})
        depot_info = _service.add_daemons_to_depot(args['depot_id'], daemon_spec_list)
    except KeyError, e:
        return TcdsApiErrorResponse(Globals.ERROR_GENERAL, 'No such depot. %s' % e)
    except Exception as e:
        return TcdsApiErrorResponse(Globals.ERROR_GENERAL, e)
    else:
        return TcdsApiSuccessResponse({'depot_info': depot_info})

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
    if 'force' not in args:
        args['force'] = False
    try:
        depot_info = _service.del_nodes_from_depot(args['depot_id'], args['node_list'], args['force'])
    except KeyError:
        return TcdsApiErrorResponse(Globals.ERROR_GENERAL, 'No such depot.')
    except Exception as e:
        return TcdsApiErrorResponse(Globals.ERROR_GENERAL, e)
    else:
        return TcdsApiSuccessResponse({'depot_info': depot_info})

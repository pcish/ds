import uuid
from depot import LocalVarStore as VariableStore
from depot import *

#global depot
depot = {}

class TCDSAPI:
    SUCCESS = 0
    ERROR_GENERAL = 1

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
        depot[NewDepotID] = Depot(NewDepotID, VariableStore(), replication_number)
    except Exception as e:
        response = {
            'result_code': TCDSAPI.ERROR_GENERAL,
            'error_message': e
        }
    else:
        response = {
            'result_code': TCDSAPI.SUCCESS,
            'depot_id': NewDepotID
        }
    return response

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
        depot_info = depot[args['depot_id']].get_info()
    except KeyError:
        response = {
            'result_code': TCDSAPI.ERROR_GENERAL,
            'error_message': 'No such depot.'
        }
    except Exception as e:
        response = {
            'result_code': TCDSAPI.ERROR_GENERAL,
            'error_message': e
        }
    else:
        response = {
            'result_code': TCDSAPI.SUCCESS,
            'depot_info': depot_info
        }
    return response

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
        depot_info = depot[args['depot_id']].add_nodes(args['node_spec_list'])
    except KeyError, e:
        print e
        response = {
            'result_code': TCDSAPI.ERROR_GENERAL,
            'error_message': 'No such depot.'
        }
    except Exception as e:
        response = {
            'result_code': TCDSAPI.ERROR_GENERAL,
            'error_message': e
        }
        print e
    else:
        response = {
            'result_code': TCDSAPI.SUCCESS,
            'depot_info': depot_info
        }
    print response
    return response

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
    try:
        depot_info = depot[args['depot_id']].remove_nodes(args['node_list'])
    except KeyError:
        response = {
            'result_code': TCDSAPI.ERROR_GENERAL,
            'error_message': 'No such depot.'
        }
    except Exception as e:
        response = {
                    'result_code': TCDSAPI.ERROR_GENERAL,
                    'error_message': e
                    }
    else:
        response = {
            'result_code': TCDSAPI.SUCCESS,
            'depot_info': depot_info
        }
    return response

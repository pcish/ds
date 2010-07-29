import service

if __name__ == '__main__':
    try:
        res = service.createDepot({'replication_number': 2,})
        print res['depot_id']
        did = res['depot_id']
    except Exception as e:
        #print res['error_message']
        print e
    try:
        info = service.getDepotInfo({'depot_id': 'i bet this id does not exist'})['depot_info']
    except:
        pass
    else:
        assert(0)
    try:
        info = service.getDepotInfo({'depot_id': did})
    except Exception, e:
        print e
    else:
        print info

    service.addStorageNodes({
        'depot_id': did,
        'node_spec_list': [
            { 'node_id': '0895d363-2972-4c40-9f5b-0df2b224a2c6', 'storage_roles': [ 'mon', 'osd' ], 'node_ip': '10.201.193.170'},
            { 'node_id': 'a0e6fbf4-e6d2-4a7a-97d3-9390703d6b3a', 'storage_roles': [ 'mon', 'osd' ] },
            { 'node_id': '92144222-e7b6-4c13-aeb8-7a32cd2c6458', 'storage_roles': [ 'mon', 'osd' ] },
            { 'node_id': 'f97f46ee-7d40-4385-b9a0-7b46079d699b', 'storage_roles': [ 'mds' ] },
            { 'node_id': 'c8634dc9-ddc6-41c4-ba12-b1d4b5523e2e', 'storage_roles': [ 'mds' ] },
        ]
    })

    daemons = service._service._depot[did].get_daemon_list()
    for daemon in daemons:
        print daemon.host, daemon.TYPE, daemon.ceph_name
    print '--------------'
    print service.removeStorageNodes({
        'depot_id': did,
        'node_list': [
            { 'node_id': '0895d363-2972-4c40-9f5b-0df2b224a2c6'}
        ],
        'force': True
    })
    daemons = service._service._depot[did].get_daemon_list()
    for daemon in daemons:
        print daemon.host, daemon.TYPE, daemon.ceph_name
    print '--------------'
    print service.addStorageNodes({
        'depot_id': did,
        'node_spec_list': [
            { 'node_id': '0895d363-2972-4c40-9f5b-0df2b224a2c6', 'storage_roles': [ 'mon' ], 'node_ip': '10.201.193.170'},
            { 'node_id': 'f0797c41-b21f-4eda-8093-32285453d035', 'storage_roles': [ 'osd' ], 'node_ip': '10.201.193.171'}
        ]
    })
    daemons = service._service._depot[did].get_daemon_list()
    for daemon in daemons:
        print daemon.host, daemon.TYPE, daemon.ceph_name
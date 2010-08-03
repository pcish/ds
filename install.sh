#!/bin/sh

ds_lib=/opt/tcloud/Elaster/lib/python2.6/site-packages/tcloud/ds/

install service.py $ds_lib
install depot.py $ds_lib
install daemon.py $ds_lib
install serviceglobals.py $ds_lib
install tccephconf.py $ds_lib
install varstore.py $ds_lib
install pahook.py $ds_lib
install -t $ds_lib/ceph ceph/*.py
install -b tcds.cfg /opt/tcloud/Elaster/etc/processagent/modules/tcds.cfg

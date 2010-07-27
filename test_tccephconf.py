import unittest
import os
from tccephconf import *

class TestTCCephConf(unittest.TestCase):
    def setUp(self):
        test_conf_contents = """
[global]
    debug = 20
\tosd journal = /var/log/journal/osd$id
[mon]
    mon data = /data/ceph/mon$id
 [mon.0]
    addr = 192.168.0.1:6789


[osd]
    osd data = /data/ceph/osd$id
[osd0]
    host = abc
[osd.1]
    osd data = /random/prefix
"""
        with open('test_ceph.conf', 'w') as f:
            f.write(test_conf_contents)
    def test_read(self):
        f = CephConfFile('test_ceph.conf')
        conf = TCCephConf()
        conf.readfp(f)
        self.assertEquals(conf.get('global', 'debug'), '20')
        self.assertEquals(conf.get('osd0', 'osd data'), '/data/ceph/osd$id')
        self.assertEquals(conf.get('osd.1', 'osd data'), '/random/prefix')
        self.assertEquals(conf.get('mon.0', 'addr'), '192.168.0.1:6789')
        self.assertEquals(conf.get('osd.1', 'osd journal'), '/var/log/journal/osd$id')

    def tearDownss(self):
        os.remove('test_ceph.conf')

class TestGetMonIpFromConfig(unittest.TestCase):

    def test_single_mon(self):
        config = TCCephConf()
        config.create_default()
        (id, hostname, ip, port) = ('0', '192.168.0.1', None, None)
        config.add_mon(id, hostname, ip, port)
        self.assertEqual(config.get_active_mon_ip(), (hostname, id))

    def test_multiple_mon(self):
        config = TCCephConf()
        config.create_default()
        (id, hostname, ip, port) = ('0', '192.168.0.1', None, None)
        config.add_mon(id, hostname, ip, port)
        (id, hostname, ip, port) = ('1', '192.168.0.2', None, None)
        config.add_mon(id, hostname, ip, port)
        (id, hostname, ip, port) = ('2', '192.168.0.3', None, None)
        config.add_mon(id, hostname, ip, port)
        self.assertEqual(config.get_active_mon_ip(), ('192.168.0.1', '0'))

    def test_missing_mon(self):
        config = TCCephConf()
        config.create_default()
        (id, hostname, ip, port) = ('2', '192.168.0.3', None, None)
        config.add_mon(id, hostname, ip, port)
        self.assertEqual(config.get_active_mon_ip(), (hostname, id))


def test_read(filename):
    f = open(filename, 'r')
    tmp = open('tmp', 'w')
    for line in f:
        tmp.write(line.lstrip())
    tmp.close()
    f.close()

def test_default():
    conf = TCCephConf()
    conf.create_default('hfaldjalfnvotij-fsd-dfvl')
    conf.add_mon('0', '10.201.193.140')
    print conf.get('mon.0', 'addr')
    print conf.get('tcloud', 'depot')
#    conf.read('tmp')
#    conf.add_section('tcloud')
#    conf.set('tcloud', 'depot id', 'hfaldjalfnvotij-fsd-dfvl')
#    for k, v in conf.defaults().iteritems():
#        print k, v
#        conf.set('global', k, v)
#    sections = conf.sections()
#    for s in sections:
#        print s
    with open('new.conf', 'wb') as outfile:
        conf.write(outfile)


if __name__ == '__main__':
    #test_read()
    unittest.main()
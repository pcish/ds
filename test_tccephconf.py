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

    def tearDown(self):
        os.remove('test_ceph.conf')

class TestGetMonIpFromConfig(unittest.TestCase):
    class FakeDaemon:
        TYPE = 'mon'
        ceph_name = ''
        def get_ceph_name(self):
            return self.ceph_name

    def test_single_mon(self):
        config = TCCephConf()
        config.create_default()
        (id, hostname, ip, port) = ('0', '192.168.0.1', None, None)
        daemon = self.FakeDaemon()
        daemon.ceph_name = id
        config.add_mon(daemon, hostname, ip, port)
        self.assertEqual(config.get_active_mon_ip(), (hostname, id))

    def test_multiple_mon(self):
        config = TCCephConf()
        config.create_default()
        (id, hostname, ip, port) = ('2', '192.168.0.3', None, None)
        daemon = self.FakeDaemon()
        daemon.ceph_name = id
        config.add_mon(daemon, hostname, ip, port)
        (id, hostname, ip, port) = ('0', '192.168.0.1', None, None)
        daemon = self.FakeDaemon()
        daemon.ceph_name = id
        config.add_mon(daemon, hostname, ip, port)
        (id, hostname, ip, port) = ('1', '192.168.0.2', None, None)
        daemon = self.FakeDaemon()
        daemon.ceph_name = id
        config.add_mon(daemon, hostname, ip, port)
        self.assertEqual(config.get_active_mon_ip(), ('192.168.0.1', '0'))

    def test_missing_mon(self):
        config = TCCephConf()
        config.create_default()
        (id, hostname, ip, port) = ('2', '192.168.0.3', None, None)
        daemon = self.FakeDaemon()
        daemon.ceph_name = id
        config.add_mon(daemon, hostname, ip, port)
        (id, hostname, ip, port) = ('1', '192.168.0.2', None, None)
        daemon = self.FakeDaemon()
        daemon.ceph_name = id
        config.add_mon(daemon, hostname, ip, port)
        self.assertEqual(config.get_active_mon_ip(), (hostname, id))


if __name__ == '__main__':
    unittest.main()
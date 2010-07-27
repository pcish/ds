import unittest
from daemon import Mon, Mds, Osd
from tccephconf import TCCephConf

class TestGetMonIpFromConfig(unittest.TestCase):
    daemon = None
    def setUp(self):
        self.daemon = Mon(None, None)

    def test_single_mon(self):
        config = TCCephConf()
        config.create_default()
        (id, hostname, ip, port) = ('0', '192.168.0.1', None, None)
        config.add_mon(id, hostname, ip, port)
        self.assertEqual(self.daemon.get_active_mon_ip_from_config(config), (hostname, id))

    def test_multiple_mon(self):
        config = TCCephConf()
        config.create_default()
        (id, hostname, ip, port) = ('0', '192.168.0.1', None, None)
        config.add_mon(id, hostname, ip, port)
        (id, hostname, ip, port) = ('1', '192.168.0.2', None, None)
        config.add_mon(id, hostname, ip, port)
        (id, hostname, ip, port) = ('2', '192.168.0.3', None, None)
        config.add_mon(id, hostname, ip, port)
        self.assertEqual(self.daemon.get_active_mon_ip_from_config(config), ('192.168.0.1', '0'))

    def test_missing_mon(self):
        config = TCCephConf()
        config.create_default()
        (id, hostname, ip, port) = ('2', '192.168.0.3', None, None)
        config.add_mon(id, hostname, ip, port)
        self.assertEqual(self.daemon.get_active_mon_ip_from_config(config), (hostname, id))

if __name__ == '__main__':
    unittest.main()
import unittest

if __name__ == '__main__':
    s = unittest.TestSuite()
    module_names = ['test_service', 'test_depot', 'test_daemon', 'test_varstore', 'test_cephconf']
    for n in module_names:
        s.addTests(unittest.defaultTestLoader.loadTestsFromName(n))
    unittest.TextTestRunner().run(s)
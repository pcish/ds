import unittest
from depot import Depot

class TestDepot(unittest.TestCase):
    def test_min_req_check(self):
        self.assertEquals(Depot._get_meets_min_requirements(3, 3, 2, 3), True)
        self.assertEquals(Depot._get_meets_min_requirements(3, 3, 2, 2), False)
        self.assertEquals(Depot._get_meets_min_requirements(3, 3, 1, 3), False)
        self.assertEquals(Depot._get_meets_min_requirements(2, 2, 3, 3), False)
        self.assertEquals(Depot._get_meets_min_requirements(3, 3, 2, 4), True)
        self.assertEquals(Depot._get_meets_min_requirements(3, **{'num_osd': 3, 'num_mon':4, 'num_mds': 1}), False)
        self.assertRaises(ValueError, Depot._get_meets_min_requirements, 0, 3, 2, 3)
        self.assertRaises(ValueError, Depot._get_meets_min_requirements, 3, -1, 2, 3)
        self.assertRaises(TypeError, Depot._get_meets_min_requirements, 3.5, 3, 2, 3)


if __name__ == '__main__':
    unittest.main()

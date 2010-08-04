import unittest
import sys, os
sys.path.append(os.path.abspath(os.path.join(__file__, '../..')))
from daemon import Mon, Mds, Osd
from cephconf import TCCephConf


if __name__ == '__main__':
    unittest.main()
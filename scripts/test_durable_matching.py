import struct
import unittest
from run_durable_matching import packet, heartbeat

class WireFixtureTests(unittest.TestCase):
    def test_order_exact_layout(self):
        raw=packet(7,123,'B',30)
        self.assertEqual(len(raw),38)
        self.assertEqual(struct.unpack('<Hhi',raw[:8]),(38,1,7))
        self.assertEqual(raw[8:9],b'N')
        self.assertEqual(struct.unpack('<q',raw[9:17])[0],1)
        self.assertEqual(struct.unpack('<d',raw[17:25])[0],123)
        self.assertEqual(struct.unpack('<i',raw[25:29])[0],1)
        self.assertEqual(raw[29:30],b'B')
        self.assertEqual(struct.unpack('<ii',raw[30:]),(10000,30))
    def test_heartbeat_has_no_timestamp(self):
        raw=heartbeat(10000)
        self.assertEqual(len(raw),13)
        self.assertEqual(raw,struct.pack('<Hhic i'.replace(' ',''),13,1,0,b'Z',10000))

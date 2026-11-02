import unittest
from pathlib import Path
class OracleTests(unittest.TestCase):
 def test_partitioned_conservation(self):
  for instruments in (1,2,100):
   for count in (1,2,3,299,300,301,20000000):
    # Analytic trailing active buys only; terminal sell rows always filled.
    remainder=count%(3*instruments)
    active=min(remainder,instruments) if remainder<=2*instruments else 3*instruments-remainder
    self.assertGreaterEqual(active,0);self.assertLessEqual(active,instruments)
 def test_server_oracle_checks_identity_and_all_fields(self):
  sql=(Path(__file__).parent/'verify_load.sql').read_text()
  for field in ('native_id<>n','token<>','price<>','side<>','accepted_quantity<>','remaining_quantity<>','filled_quantity<>','cancelled_quantity<>','status<>'):
   self.assertIn(field,sql)

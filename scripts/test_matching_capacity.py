import copy
import unittest
from run_matching_capacity import validate

class CapacityTests(unittest.TestCase):
    def fixture(self):
        samples=[dict(phase=p,active_orders=100,heap_alloc_bytes=10000,rss_bytes=20000)
                 for p in ('loaded_gc','churn_gc','restored_gc')]
        samples += [dict(phase=p,active_orders=0) for p in ('final_drained_gc','released')]
        return dict(version=1,count=100,layout='dense',os='linux',correctness_verified=True,
                    database_style_restore_verified=True,samples=samples)
    def test_valid(self):
        self.assertEqual(validate(self.fixture(),100,'dense')['count'],100)
    def test_reject_incorrect_or_incomplete(self):
        for field,value in [('count',99),('layout','sparse'),('os','darwin'),
                            ('correctness_verified',False),('database_style_restore_verified',False)]:
            result=copy.deepcopy(self.fixture());result[field]=value
            with self.assertRaises(ValueError):validate(result,100,'dense')
    def test_reject_unmeasured_or_wrong_count(self):
        for field,value in [('active_orders',99),('rss_bytes',0),('heap_alloc_bytes',0)]:
            result=self.fixture();result['samples'][0][field]=value
            with self.assertRaises(ValueError):validate(result,100,'dense')
    def test_reject_failed_drain(self):
        result=self.fixture();result['samples'][-1]['active_orders']=1
        with self.assertRaises(ValueError):validate(result,100,'dense')

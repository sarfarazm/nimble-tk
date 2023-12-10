import unittest
import pandas as pd

class TestPandasUtils(unittest.TestCase):

    def test_rsort(self):
        df = pd.DataFrame({'A':[1,2,3,4], 'B':['a', 'b', 'c', 'd']})
        df_reverse_sorted = pd.DataFrame({'A':[4, 3, 2, 1], 'B':['d', 'c', 'b', 'a']})
        df_r = df.rsort('A').reset_index(drop=True)
        self.assertTrue(df_r.equals(df_reverse_sorted))
    
    def test_vcp(self):
        df = pd.DataFrame({'A':[1, 2, 3, 4, 5], 'B':['a', 'a', 'a', 'b', 'b']})
        df_ref = pd.DataFrame({'COUNT':[3, 2], 'PERC':[.6, .4]}, index=['a', 'b'])
        df_vcp = df.B.vcp()
        self.assertTrue(df_vcp.equals(df_ref))

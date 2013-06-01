from __future__ import unicode_literals

from unittest import TestCase

from solar import func


class FuncTest(TestCase):
    def test_func(self):
        f = func.recip(func.sum('rank',10), 1, 1, 0)
        self.assertEqual(str(f), 'recip(sum(rank,10),1,1,0)')

        f = func.linear('rank', func.sqrt(func.sum(func.pow('x'), func.pow('y'))), 10)
        self.assertEqual(str(f), 'linear(rank,sqrt(sum(pow(x),pow(y))),10)')

        f = func.if_(func.exists('name'), 100, 0)
        self.assertEqual(str(f), 'if(exists(name),100,0)')

        f = func.termfreq('name', "test term '111'")
        self.assertEqual(str(f), "termfreq(name,'test term \\'111\\'')")

        f = func.undefined('name', 'null')
        self.assertEqual(str(f), 'undefined(name,null)')

        f = func.exists('name', 5, 0) * 2 + func.ord('popularity') * 0.5 + func.recip(func.rord('price'), 1, 1000, 1000) * 0.3
        self.assertEqual(str(f), 'exists(name,5,0)^2 ord(popularity)^0.5 recip(rord(price),1,1000,1000)^0.3')

        f = func.min(func.geodist('store', 37.7, -122.4), func.geodist('store', 39.7, -105))
        self.assertEqual(str(f), 'min(geodist(store,37.7,-122.4),geodist(store,39.7,-105))')

        self.assertRaises(TypeError, lambda: func.min('a', 'b') * '10')
        self.assertRaises(TypeError, lambda: func.min('a', 'b') + '10')
        self.assertRaises(TypeError, lambda: func.min('a', 'b') + func.max('a', 'b') + '10')

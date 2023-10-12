import unittest
import datetime
import seaborn as sns
import matplotlib.pyplot as plt

import pandas as pd
from pandas.core.resample import TimeGrouper
import mplfinance as mpf

from multiplex import PlexClient


class BasicStorageTestCase(unittest.TestCase):
    def test_add_record(self):
        s = PlexClient([], None)

        s.add_record({'epic': 'CC.D.CL.UMP.IP', 'ask': 8730.8, 'bid': 8728.0,
                      't': datetime.datetime(2022, 10, 6, 13, 45, 6, 127174)})
        s.add_record({'epic': 'CC.D.CL.UMP.IP', 'ask': 26302.0, 'bid': 26262.0,
                      't': datetime.datetime(2022, 10, 6, 13, 45, 6, 196581)})
        s.add_record({'epic': 'CC.D.CL.UMP.IP', 'ask': 8731.4, 'bid': 8727.6,
                      't': datetime.datetime(2022, 10, 6, 13, 45, 6, 302608)})

        self.assertEqual(len(s.tables['CC.D.CL.UMP.IP']), 3,
                         msg='Storage adds three sequential records to the same epic')

        s.add_record({'epic': 'CC.D.CL.UMP.IP', 'ask': 8730.8, 'bid': 8728.0,
                      't': datetime.datetime(2022, 10, 6, 13, 45, 6, 127174)})

        self.assertEqual(len(s.tables['CC.D.CL.UMP.IP']), 3,
                         msg='Storage ignores older record ')

        s.add_record({'epic': 'CC.D.RB.UMP.IP', 'ask': 8730.8, 'bid': 8728.0,
                      't': datetime.datetime(2022, 10, 6, 13, 45, 6, 127174)})

        self.assertEqual(len(s.tables), 2,
                         msg='Storage adds new table for each epic.')

    def test_get_records(self):
        s = PlexClient([], None)

        s.add_record({'epic': 'CC.D.CL.UMP.IP', 'ask': 8730.8, 'bid': 8728.0,
                      't': datetime.datetime(2022, 10, 6, 13, 45, 6, 127174)})
        s.add_record({'epic': 'CC.D.CL.UMP.IP', 'ask': 8730.8, 'bid': 8728.0,
                      't': datetime.datetime(2022, 10, 6, 13, 45, 6, 147174)})
        s.add_record({'epic': 'CC.D.CL.UMP.IP', 'ask': 26302.0, 'bid': 26262.0,
                      't': datetime.datetime(2022, 10, 6, 13, 45, 6, 196581)})
        s.add_record({'epic': 'CC.D.CL.UMP.IP', 'ask': 26302.0, 'bid': 26262.0,
                      't': datetime.datetime(2022, 10, 6, 13, 45, 6, 199581)})
        s.add_record({'epic': 'CC.D.CL.UMP.IP', 'ask': 8731.4, 'bid': 8727.6,
                      't': datetime.datetime(2022, 10, 6, 13, 45, 6, 302608)})

        records = s.get_records(
            'CC.D.CL.UMP.IP',
            datetime.datetime(2022, 10, 6, 13, 45, 6, 147174),
            datetime.datetime(2022, 10, 6, 13, 45, 6, 199581)
        )

        self.assertEqual(len(records), 3, msg='Storage returns correct number of rows for range query')

    def test_ma(self):

        feature = 'ask'
        # epic = "CS.D.GBPJPY.MINI.IP"
        # epic = "CC.D.RB.UMP.IP"
        # epic = "CC.D.CL.UMP.IP"
        epic = "IX.D.DAX.IFS.IP"
        # epic = "CC.D.HO.UMP.IP"
        ma_0 = 10
        ma_1 = 20
        interval = '10s'
        start = '2022-10-14 17:30:00'
        end = '2022-10-14 18:00:00'

        df = pd.read_json('/Users/conorturner/datasets/history.jsonl', lines=True, convert_dates=False)
        df['t'] = pd.to_datetime(df['date'] + ' ' + df['t'])
        df = df[df.epic == epic]
        df = df.set_index('t')

        df = df[start:end]

        print(df)

        # print(df.groupby(TimeGrouper(freq='Min')).mean().rolling(window=3).mean())

        # fig = plt.figure(figsize=(10, 10))
        mean = df.resample(interval).mean().fillna(method='ffill')
        spread = mean['ask'] - mean['bid']
        center = (mean['ask'] + mean['bid']) / 2
        ochl = ((df['ask'] + df['bid']) / 2).resample(interval).ohlc(_method='ohlc').fillna(method='ffill')
        # ochl = df[feature].resample('1Min').ohlc(_method='ohlc').fillna(method='ffill')

        ma5 = mean.fillna(method='ffill').rolling(window=ma_0).mean()[feature]
        ma10 = mean.fillna(method='ffill').rolling(window=ma_1).mean()[feature]

        above_ma = ((ma5 - ma10) > 0)
        changes = (above_ma != above_ma.shift(1))[1:]
        change_idx = changes[changes].index.values

        spread_fill = {
            'y1': center.values + spread.values,
            'y2': center.values - spread.values,
            'alpha': 0.2
        }

        fig, axlist = mpf.plot(ochl,
                               title=epic,
                               type='candle',
                               mav=(ma_0, ma_1), returnfig=True,
                               fill_between=[spread_fill]
                               )
        axlist[0].legend(('HL', 'OC', ma_0, ma_1, 'Spread'))
        for i in range(len(change_idx)):

            is_above = above_ma.loc[change_idx[i]]
            d_idx = ochl.index.get_indexer([change_idx[i]])[0]
            if i > len(change_idx) - 2:
                d_end_idx = len(ochl.index)
            else:
                d_end_idx = ochl.index.get_indexer([change_idx[i + 1]])[0]
            axlist[0].axvspan(d_idx, d_end_idx, facecolor='g' if is_above else 'r', alpha=0.1)

        mpf.show()


if __name__ == '__main__':
    unittest.main()

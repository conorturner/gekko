import json
import unittest
import math
import aiofiles
import pandas as pd
from sortedcontainers import SortedDict
from tqdm.asyncio import tqdm
from datetime import datetime


class OCHLUnit:
    o = None
    c = 0
    h = -math.inf
    l = math.inf

    def __iter__(self):
        return iter([self.o, self.c, self.h, self.l])

    def update(self, value):
        self.c = value  # close is the most recent value
        self.h = max(self.h, value)
        self.l = min(self.l, value)
        if self.o is None:
            self.o = value


truncation_map = dict(
    s=dict(),
    m=dict(second=0),
    h=dict(minute=0, second=0),
    d=dict(minute=0, hour=0, second=0),
)


class OCHLBuffer:
    def __init__(self, max_depth=3600 * 24 * 5, frequencies=('s', 'm', 'h', 'd')):
        self.buffers = {f: SortedDict() for f in frequencies}
        self.max_depth = max_depth

    def dataframes(self):
        print(pd.DataFrame([(k,) + tuple(r) for k, r in self.buffers['h'].items()], columns=['t', 'o', 'c', 'h', 'l']))

    def append(self, row):
        for freq, buffer in self.buffers.items():
            t = datetime.strptime(row['t'], '%Y-%m-%d %H:%M:%S').replace(**truncation_map[freq])
            buffer = self.buffers[freq]

            if t not in buffer:
                buffer[t] = OCHLUnit()

            if len(buffer) > self.max_depth:
                removed, _ = buffer.popitem(0)

                if removed == t:  # sometimes very stale data is sent to endpoint
                    return

            buffer[t].update((row['ask'] + row['bid']) / 2)


class OCHLStream:
    buffers: dict

    def __init__(self, frequencies=('s', 'm', 'h', 'd'), max_depth=3600 * 24 * 5):
        self.buffers = {}
        self.max_depth = max_depth
        self.frequencies = frequencies

    def append(self, row):
        if row['epic'] not in self.buffers:
            buffer = self.buffers[row['epic']] = OCHLBuffer(frequencies=self.frequencies, max_depth=self.max_depth)
        else:
            buffer = self.buffers[row['epic']]

        buffer.append(row)


async def dummy_market_stream():
    async with aiofiles.open('/Users/conorturner/datasets/history.jsonl', mode='r') as f:
        async for line in f:
            row = json.loads(line)
            row['t'] = f"{row['date']} {row['t']}"
            yield row


class OCHLBufferTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_history(self):
        buffer = OCHLStream(frequencies=('m',), max_depth=500)
        async for row in tqdm(dummy_market_stream(), total=8716138):
            buffer.append(row)
            # print(row)
            # break
        print(list(buffer.buffers.keys()))
        print(buffer.buffers['CC.D.CL.UMP.IP'].dataframes())


if __name__ == '__main__':
    unittest.main()

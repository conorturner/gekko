import datetime
import logging
import time

import pandas as pd

from modules.core import Gekko


class MartingaleGekko(Gekko):
    epic: str
    current_size = 0.5
    current_direction = 'SELL'

    def __init__(self, epic, ig_service, price_stream, **kwargs):
        super(MartingaleGekko, self).__init__(ig_service, price_stream, **kwargs)
        self.epic = epic

    async def on_tick(self):
        now = datetime.datetime.now()
        start = now - datetime.timedelta(seconds=self.tick)
        rows = self.price_stream.get_records(self.epic, start, now)

        if len(rows) < 2:
            return

        if len(self.market.trades) != 0:
            deal_id = list(self.market.trades.keys())[0]
            profit = self.market.close(deal_id)['profit']
            if profit >= 0:
                self.current_direction = 'SELL' if self.current_direction == 'BUY' else 'BUY'
                self.current_size = max(0.5, self.current_size / 2.0)
            else:
                self.current_size = min(4.0, self.current_size * 2.0)

        self.market.open(self.epic, self.current_direction, size=self.current_size)


class MAScalper(Gekko):
    epic: str

    def __init__(self, epic, fast_ma, slow_ma, limit, stop, ig_service, price_stream, **kwargs):
        super(MAScalper, self).__init__(ig_service, price_stream, **kwargs)
        assert fast_ma < slow_ma

        self.epic = epic
        self.fast_ma = fast_ma
        self.slow_ma = slow_ma
        self.limit = limit
        self.stop = stop
        self.log = logging.getLogger(f'MAScalper({epic})')
        self.previous_is_above = None
        self.open_deal_id = None

    async def on_tick(self):
        init = time.time()
        now = datetime.datetime.now()
        start = now - datetime.timedelta(seconds=self.tick)
        rows = self.price_stream.get_records(self.epic, end=start)

        if len(rows) < 2:
            self.log.info(f'Not enough data {self.epic} {len(rows)}')
            return

        df = pd.DataFrame(rows)
        df['t'] = pd.to_datetime(df['t'])
        df = df.set_index('t')

        center = (df['ask'] + df['bid']) / 2
        resample = center.resample(f'{self.tick}S').mean().fillna(method='ffill')

        if len(resample) < self.slow_ma + 1 and len(resample) % 10 == 2:
            self.log.info(f'Collecting Data {len(resample)}/{self.slow_ma}')
        else:
            fast = resample.rolling(window=self.fast_ma).mean()
            slow = resample.rolling(window=self.slow_ma).mean()
            is_above = (fast > slow).iloc[-1]

            if self.previous_is_above is not None and self.previous_is_above != is_above:
                try:
                    direction = 'BUY' if is_above else 'SELL'
                    # TODO: try trailing stop (reduces possible wins but maybe losses by a greater amount)
                    if self.open_deal_id is not None:
                        await self.market.close(self.open_deal_id)

                    result = await self.market.open(self.epic, direction, limit=self.limit, stop=self.stop, size=1.0)
                    self.open_deal_id = result['dealId']

                except Exception as e:
                    self.log.exception(e)

            self.previous_is_above = is_above

        self.log.info(f'Duration {round(time.time() -init, ndigits=4)}s')


class GekkoFactory:
    def __init__(self, ig_api, price_stream):
        self.ig_api = ig_api
        self.price_stream = price_stream

    def ma_cross(self, epic, fast=30, slow=60, limit=1, stop=12, tick=10):
        return MAScalper(epic, fast, slow, limit, stop, self.ig_api, self.price_stream, tick=tick)

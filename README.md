# Gekko - A Light Weight Online Trading Frameowork for IG Streaming API

## Environment Variables

The following ENV vars must be set: IG_API_KEY, IG_ACCOUNT, IG_USER, IG_PASS.

## Adding a new Strategy

To create a new strategy the Gekko class must be extended and the on_tick abstract method implemented.
An example of a martingale strategy is shown below.

```python
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
```
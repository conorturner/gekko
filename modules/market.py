import logging


class Market:
    trades: dict

    def __init__(self, ig_api, price_stream):
        self.ig_api = ig_api
        self.price_stream = price_stream
        self.trades = {}
        self.log = logging.getLogger('Market')

    async def open(self, epic, direction='BUY', size=1.0, limit=None, stop=None):
        result = await self.ig_api.open_position(direction, size, epic, limit=limit, stop=stop)

        if result['status'] != 'OPEN':
            raise Exception(result)

        self.log.info(
            f"{result['epic']} \t {result['status'] + '  '} \t {result['dealId']} \t {direction} {size}@{result['level']}")

        self.trades[result['dealId']] = {
            'epic': result['epic'],
            'entry': result['level'],
            'size': size,
            'direction': direction
        }
        return result

    async def close(self, deal_id, size=None):
        if deal_id not in self.trades:
            raise Exception(f'Unknown deal_id: {deal_id}')

        trade = self.trades[deal_id]
        size = trade['size'] if size is None else size
        direction = 'SELL' if trade['direction'] == 'BUY' else 'BUY'

        result = await self.ig_api.close_position(deal_id, size, direction)

        if result['status'] != 'CLOSED':
            print('open', trade['direction'], 'close', direction)
            print('error closing', result)
            print('trade', trade)
            raise Exception(result)

        del self.trades[deal_id]

        self.log.info(
            f"{result['epic']} \t {result['status']} \t {result['dealId']} \t P/L: {str(result['profit'])}")

        return result

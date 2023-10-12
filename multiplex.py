import asyncio
import datetime
import os

import aiohttp
import logging
from aiohttp import web
from asyncio import Queue
from typing import List, Dict
from trading_ig import IGService, IGStreamService
from trading_ig.lightstreamer import Subscription

from modules.logger import setup_logging

IG_API_KEY = os.environ.get('IG_API_KEY')
IG_ACCOUNT = os.environ.get('IG_ACCOUNT')
IG_USER = os.environ.get('IG_USER')
IG_PASS = os.environ.get('IG_PASS')


def async_adapter():
    """
    :return: Async Iterator, Callable
    """
    loop = asyncio.get_event_loop()
    queue = asyncio.Queue()

    def put(*args):
        loop.call_soon_threadsafe(queue.put_nowait, *args)

    async def get():
        while True:
            yield await queue.get()

    return get(), put


async def stream_ig(epics, ig_service, parse_date=True):
    stream_get, stream_put = async_adapter()

    ig_stream_service = IGStreamService(ig_service)
    # ig_stream_service.create_session()
    ig_stream_service.create_session(version='3')

    # Making a new Subscription in MERGE mode
    subscription_prices = Subscription(
        mode="MERGE",
        items=[f"MARKET:{epic}" for epic in epics],  # sample CFD epics
        fields=["BID", "OFFER", "UPDATE_TIME"],
    )

    # Adding the "on_price_update" function to Subscription
    subscription_prices.addlistener(stream_put)
    sub_key_prices = ig_stream_service.ls_client.subscribe(subscription_prices)

    async for item in stream_get:
        today = datetime.datetime.now()
        try:
            hour, minutes, seconds = tuple(map(int, item['values']['UPDATE_TIME'].split(':')))
            today = today.replace(hour=hour, minute=minutes, second=seconds, microsecond=0)
            yield {
                'epic': item['name'].split(':')[1],
                'ask': float(item['values']['OFFER']),
                'bid': float(item['values']['BID']),
                't': today if parse_date else item['values']['UPDATE_TIME']
            }
        except ValueError as e:
            print('Error reading stream', e)

    # Disconnecting
    # ig_stream_service.disconnect()



class IGMultiplex:
    epics: List[str]
    queues: Dict[str, Queue]
    subscriptions: Dict[str, set]
    buffers: Dict[str, list]

    def __init__(self, epics):
        self.app = web.Application()
        self.app.add_routes([web.get('/', self.ws_connect)])
        self.epics = epics
        self.queues = {e: Queue() for e in epics}
        self.subscriptions = {e: set() for e in epics}
        self.buffers = {e: list() for e in epics}
        self.ig_service = IGService(
            IG_USER, IG_PASS, IG_API_KEY, "DEMO",
            acc_number=IG_ACCOUNT
        )
        self.log = logging.getLogger('IGMultiplex')

    async def heartbeat(self):
        while True:
            self.log.info(' - '.join([f'{k}:{len(t)}' for k, t in self.buffers.items()]))
            await asyncio.sleep(5)

    async def ws_connect(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        # TODO: Expect a subscribe message to begin every connection
        # await ws.receive_json()

        self.log.info(f'New WS Connection {request.remote}')

        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                if msg.data == 'close':
                    await ws.close()
                else:
                    data = msg.json()
                    if 'subscribe' in data:
                        for e in data['subscribe']:
                            if e not in self.epics:
                                continue
                            await ws.send_json({'epic': e, 'history': self.buffers[e]})
                            self.subscriptions[e].add(ws)
            elif msg.type == aiohttp.WSMsgType.ERROR:
                self.log.warning('Websocket exited with error')
                self.log.exception(ws.exception())

        self.log.info(f'WS connection closed {request.remote}')

        for e in self.epics:
            if ws in self.subscriptions[e]:
                self.subscriptions[e].remove(ws)

        return ws

    async def ingress(self):
        gen = stream_ig(self.epics, self.ig_service)
        self.log.info('Got IG Stream')
        async for event in gen:
            await self.queues[event['epic']].put(event)

    async def consume_queue(self, epic):
        while True:
            item = await self.queues[epic].get()
            item['t'] = str(item['t'])
            self.buffers[epic].append(item)
            for ws in self.subscriptions[epic]:
                await ws.send_json(item)

    async def egress(self):
        await asyncio.gather(*[self.consume_queue(e) for e in self.epics])

    async def listen(self):
        port = 8080
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', port)
        await site.start()
        self.log.info(f'Websocket server listening at ws://localhost:{port}')
        while True:
            await asyncio.sleep(3600)

    async def start(self):
        await asyncio.gather(
            self.ingress(),
            self.egress(),
            self.listen(),
            self.heartbeat(),
        )


def str_to_dt(s):
    return datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
    # return datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S.%f')


class PlexClient:
    tables: Dict[str, list]

    def __init__(self, epics, port=8080):
        self.port = port
        self.log = logging.getLogger('PlexClient')
        self.epics = epics
        self.tables = {e: [] for e in epics}

    async def connect(self):
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(f'ws://localhost:{self.port}') as ws:
                await ws.send_json({"subscribe": self.epics})

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        if msg.data == 'close cmd':
                            await ws.close()
                            break
                        else:
                            data = msg.json()
                            if 'history' in data:
                                self.update_history(data['epic'], data['history'])
                            else:
                                self.add_record(data)
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        self.log.exception(ws.exception())

    def update_history(self, epic, history):
        for r in history:
            print(r['t'])
            r['t'] = str_to_dt(r['t'])

        self.tables[epic] = history

    def add_record(self, r: dict):
        r['t'] = str_to_dt(r['t'])
        table = self.tables[r['epic']]
        if len(table) != 0 and table[-1]['t'] > r['t']:
            return
        else:
            table.append(r)

    def get_records(self, epic: str, start=None, end=None):
        if epic not in self.tables:
            return []
        return list(filter(
            lambda r: (start is None or start <= r['t']) and (end is None or r['t'] <= end),
            self.tables[epic]
        ))

    def describe(self):
        return dict([(k, len(v)) for k, v in self.tables.items()])


async def main():
    epics = [
        "CS.D.CRYPTOB10.CFD.IP",  # Crypto 10 Index
        "CS.D.BCHUSD.CFD.IP",  # BTC Cash
        "CS.D.UNIUSD.CFD.IP",  # UniSwap
        "CC.D.RB.UMP.IP", "CC.D.HO.UMP.IP", "CC.D.CL.UMP.IP",  # US oil derivatives
        "CS.D.AUDGBP.MINI.IP",  # AUD/GBP Mini
        "IX.D.DAX.IFS.IP",  # DAX Germany 40
        "IX.D.SUNNAS.IFS.IP",  # US Tech 100 Weekend
    ]
    plex = IGMultiplex(epics)
    setup_logging()
    # client = PlexClient(["CS.D.CRYPTOB10.CFD.IP"])

    await asyncio.gather(
        plex.start(),
        # client.connect()
    )


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())

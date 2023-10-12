import asyncio
import time
from abc import ABC, abstractmethod
from modules.market import Market
from multiplex import PlexClient


class Gekko(ABC):
    price_stream: PlexClient
    tick: float

    def __init__(self, ig_service, price_stream, tick=1.0):
        self.ig_service = ig_service
        self.price_stream = price_stream
        self.tick = tick
        self.market = Market(ig_service, price_stream)

    @abstractmethod
    async def on_tick(self):
        # It is recommended that any heavy computation is done in a child process
        pass

    async def start(self):
        await asyncio.gather(
            self.metronome()
        )

    async def metronome(self):
        await asyncio.sleep(self.tick)  # Warmup tick
        while True:
            t0 = time.time()
            await self.on_tick()
            diff = time.time() - t0
            await asyncio.sleep(max(0.0, self.tick - diff))  # wait for remainder of tick

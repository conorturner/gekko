import asyncio
import os

from modules.logger import setup_logging
from modules.ig_api import IgAPI
from modules.models import GekkoFactory
from market.main import PlexClient

IG_API_KEY = os.environ.get('IG_API_KEY')
IG_ACCOUNT = os.environ.get('IG_ACCOUNT')
IG_USER = os.environ.get('IG_USER')
IG_PASS = os.environ.get('IG_PASS')


async def main():
    setup_logging()

    epics = [
        "CS.D.CRYPTOB10.CFD.IP",  # Crypto 10 Index
        "CS.D.BCHUSD.CFD.IP",  # BTC Cash
        "CS.D.UNIUSD.CFD.IP",  # UniSwap
        "CC.D.RB.UMP.IP", "CC.D.HO.UMP.IP", "CC.D.CL.UMP.IP",  # US oil derivatives
        "CS.D.AUDGBP.MINI.IP",  # AUD/GBP Mini
        "IX.D.DAX.IFS.IP",  # DAX Germany 40
        "IX.D.SUNNAS.IFS.IP",  # US Tech 100 Weekend
    ]

    # Single price stream object allows multiple models to use single connection
    price_stream = PlexClient(epics)

    ig_api = IgAPI(IG_API_KEY, IG_ACCOUNT, IG_USER, IG_PASS)
    factory = GekkoFactory(ig_api, price_stream)

    await asyncio.gather(
        price_stream.connect(),
        factory.ma_cross("CS.D.CRYPTOB10.CFD.IP", fast=100, slow=200, limit=None, stop=None, tick=1).start(),
        # factory.ma_cross("IX.D.DAX.IFS.IP", fast=100, slow=200, limit=10, stop=6, tick=1).start(),
        # factory.ma_cross("CS.D.AUDGBP.MINI.IP", fast=200, slow=300, limit=6, stop=10, tick=1).start()
        # MartingaleGekko("CS.D.AUDGBP.MINI.IP", ig_service, price_stream, tick=60).start()
    )


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())

import asyncio
import datetime
import json
import os

import aiofiles
import logging
from trading_ig import IGService

from modules.logger import setup_logging
from multiplex import stream_ig

setup_logging()

IG_API_KEY = os.environ.get('IG_API_KEY')
IG_ACCOUNT = os.environ.get('IG_ACCOUNT')
IG_USER = os.environ.get('IG_USER')
IG_PASS = os.environ.get('IG_PASS')


async def main():
    log = logging.getLogger('Create History')
    epics = [
        "CC.D.RB.UMP.IP", "CC.D.HO.UMP.IP", "CC.D.CL.UMP.IP",  # US oil derivatives
        "CC.D.LCO.UMP.IP",  # Brent Crude
        "CS.D.CFPGOLD.CFP.IP",  # Gold
        "CC.D.NG.UMP.IP",  # US Natural Gas
        "CS.D.GBPUSD.MINI.IP",  # GBPUSD Mini
        "CS.D.EURUSD.MINI.IP",  # EURUSD Mini
        "CS.D.USDJPY.MINI.IP",  # USDJPY Mini
        "CS.D.USDCHF.MINI.IP",  # USDCHF Mini
        "CS.D.USDCAD.MINI.IP",  # USDCAD Mini
        "CS.D.AUDGBP.MINI.IP",  # AUD/GBP Mini
        "CS.D.AUDUSD.MINI.IP",  # AUDUSD Mini
        "IX.D.DAX.IFS.IP",  # DAX Germany 40
    ]

    assert len(epics) < 40

    log.info(f'Streaming {len(epics)} instruments.')

    ig_service = IGService(
        IG_USER, IG_PASS, IG_API_KEY, "DEMO",
        acc_number=IG_ACCOUNT
    )
    async with aiofiles.open('test/history.jline', "w") as out:
        i = 0
        byte_count = 0
        async for msg in stream_ig(epics, ig_service, parse_date=False):
            msg['t'] = str(msg['t'])
            msg['date'] = datetime.datetime.now().strftime('%Y-%m-%d')

            if i % 100 == 0:
                log.info(f'Recorded {i} events ({byte_count} bytes).')
            i += 1
            s = json.dumps(msg) + '\n'
            byte_count += len(s)
            await out.write(s)
            await out.flush()


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())

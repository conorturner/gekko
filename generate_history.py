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
IG_ENV = os.environ.get('IG_ENV')


async def main():
    log = logging.getLogger('Create History')
    epics = [
        "CC.D.LCO.UMP.IP",
        "CC.D.CL.UMP.IP",
        "CC.D.RB.UMP.IP",
        "CC.D.HO.UMP.IP",
        "CC.D.C.UMP.IP",
        "CC.D.BO.UMP.IP",
        "CC.D.W.UMP.IP",
        "CC.D.NG.UMP.IP",
        "CS.D.COPPER.CFD.IP",
        "CS.D.CFDGOLD.CFDGC.IP",
        "IR.D.FLG.FWM1.IP",
        "IR.D.10YEAR100.FWM2.IP",
        # "IR.D.FGBL.FN2B.IP",
        "IR.D.JGB.FWM2.IP",
        "IR.D.FOAT.FWM2.IP",
        "IR.D.FBTS.FWM2.IP",
        "CS.D.GBPUSD.MINI.IP",
        "CS.D.EURUSD.MINI.IP",
        "CS.D.USDJPY.MINI.IP",
        "CS.D.AUDUSD.MINI.IP",
        "CS.D.EURGBP.MINI.IP",
        "CS.D.EURJPY.MINI.IP",
        "CS.D.USDCAD.MINI.IP",
        "CS.D.CRYPTOB10.CFD.IP",
        "CC.D.DX.UMP.IP",
        "CS.D.EURCHF.MINI.IP",
        "IX.D.NASDAQ.IFS.IP",
        "IX.D.FTSE.IFM.IP",
        "IX.D.DAX.IFS.IP",
        "IX.D.SPTRD.IFS.IP",
        "CC.D.VIX.UMP.IP",
        "IX.D.HSTECH.IFU.IP",
        "IX.D.XINHUA.IFM.IP",
        "CC.D.VSTOXX.UNC.IP",
    ]

    assert len(epics) <= 40

    log.info(f'Streaming {len(epics)} instruments.')

    ig_service = IGService(
        IG_USER, IG_PASS, IG_API_KEY, IG_ENV,
        acc_number=IG_ACCOUNT
    )
    async with aiofiles.open('test/40-instruments-live.jline', "w") as out:
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

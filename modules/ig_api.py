import logging
import aiohttp
import asyncio
from datetime import datetime
import requests
import json
from cachetools import TTLCache
import pandas as pd

cache = TTLCache(maxsize=10, ttl=60)
store = {}


async def refresh_token(r_token, api_key):
    url = "https://demo-api.ig.com/gateway/deal/session/refresh-token"

    payload = json.dumps({
        "refresh_token": r_token
    })
    headers = {
        'X-IG-API-KEY': api_key,
        'Version': '1',
        'Content-Type': 'application/json'
    }

    async with aiohttp.ClientSession() as session:
        async with session.request("POST", url, headers=headers, data=payload) as resp:
            return await resp.json()


def parse_ig_point(point):
    if 'ask' not in point['openPrice'] or 'bid' not in point['openPrice']:
        return None

    return {
        'time_stamp': datetime.fromisoformat(point['snapshotTimeUTC']),
        'open_ask': point['openPrice']['ask'],
        'open_bid': point['openPrice']['bid'],
        'close_ask': point['closePrice']['ask'],
        'close_bid': point['closePrice']['bid'],
        'high_ask': point['highPrice']['ask'],
        'high_bid': point['highPrice']['bid'],
        'low_ask': point['lowPrice']['ask'],
        'low_bid': point['lowPrice']['bid'],
        'volume': point['lastTradedVolume']
    }


class IgAPI:
    def __init__(self, api_key, account_id, username, password):
        self.api_key = api_key
        self.username = username
        self.password = password
        self.account_id = account_id
        self.url = "https://demo-api.ig.com/gateway/deal"
        self.log = logging.getLogger('IgAPI')

    async def _token(self):
        if 'access_token' in cache:
            return cache['access_token']
        elif 'refresh_token' in store:
            r = await refresh_token(store['refresh_token'], self.api_key)
            store['refresh_token'] = r['refresh_token']
            cache['access_token'] = r['access_token']
            return r['access_token']
        else:
            url = "https://demo-api.ig.com/gateway/deal/session"

            payload = json.dumps({
                "identifier": self.username,
                "password": self.password
            })
            headers = {
                'X-IG-API-KEY': self.api_key,
                'Version': '3',
                'Content-Type': 'application/json'
            }

            response = requests.request("POST", url, headers=headers, data=payload)

            resp = response.json()
            store['refresh_token'] = resp['oauthToken']['refresh_token']
            cache['access_token'] = resp['oauthToken']['access_token']

            return cache['access_token']

    async def headers(self, version=2):
        return {
            'X-IG-API-KEY': self.api_key,
            'Version': str(version),
            'IG-ACCOUNT-ID': self.account_id,
            'Authorization': f'Bearer {await self._token()}'
        }

    async def make_request(self, version, method, path, **kwargs):
        headers = await self.headers(version)
        if 'headers' in kwargs:
            headers.update(kwargs['headers'])
            del kwargs['headers']

        async with aiohttp.ClientSession() as session:
            async with session.request(method, self.url + path, headers=headers, **kwargs) as resp:
                try:
                    return await resp.json()
                except:
                    self.log.error(await resp.text())
                    raise

    async def get_positions(self):
        return await self.make_request(2, "GET", '/positions')

    async def search_market(self, term):
        return await self.make_request(1, "GET", '/markets', params={'searchTerm': term})

    async def get_deal_details(self, deal_reference):
        return await self.make_request(1, "GET", f'/confirms/{deal_reference}')

    async def open_position(self, direction, size, epic, expiry='-', limit=None, stop=None, currency='GBP'):
        payload = {
            "direction": direction,
            "size": size,
            "orderType": "MARKET",
            "epic": epic,
            "currencyCode": currency,
            "expiry": expiry,
            "forceOpen": True,
            "guaranteedStop": False,
            "limitDistance": limit,
            "stopDistance": stop,
        }
        resp = await self.make_request(2, "POST", '/positions/otc', json=payload)
        return await self.get_deal_details(resp['dealReference'])

    async def get_historical_data(self, epic, resolution='DAY', max_values=100, start='2022-02-01', end='2022-02-02'):
        resp = await self.make_request(3, "GET", f'/prices/{epic}',
                                       params={'resolution': resolution,
                                               'from': start + "T00:00:00",
                                               'to': end + "T00:00:00",
                                               'max': max_values,
                                               'pageSize': 0})

        if 'prices' not in resp or len(resp['prices']) == 0:
            return

        points = [parse_ig_point(point) for point in resp['prices']]
        return pd.DataFrame(list(filter(None, points)))

    async def close_position(self, deal_id, size, direction):
        payload = {
            "dealId": deal_id,
            "epic": None,
            "expiry": None,
            "direction": direction,
            "size": size,
            "level": None,
            "orderType": "MARKET",
            "timeInForce": None,
            "quoteId": None
        }

        resp = await self.make_request(1, "POST", '/positions/otc',
                                       headers={'_method': 'DELETE', }, json=payload)
        details = await self.get_deal_details(resp['dealReference'])
        while details['status'] != 'CLOSED':
            await self.get_deal_details(resp['dealReference'])

        return details

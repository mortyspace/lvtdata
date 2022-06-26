import asyncio
import json
import random

from datetime import datetime, timezone, timedelta
from pathlib import Path

import httpx
import aiolimiter

from web3 import Web3

API_LIMITER = aiolimiter.AsyncLimiter(3, 2)
TOTAL = 0

def get_settings(path: Path = Path('settings.json')) -> dict:
    return json.loads(path.read_text())


async def get_json(url: str, params: dict):
    data = None
    retries = 0
    while not data:
        try:
            async with httpx.AsyncClient() as cl:
                response = await cl.get(url, params=params)
                data = response.json()
        except Exception as exc:
            print('ouch', retries, str(exc))
            retries += 1
            if retries == 200:
                raise
            await asyncio.sleep(1)
    return data


async def get_cqt_request(settings: dict, url, params: dict = None):
    params = params or {}
    url = url.lstrip('/').rstrip('/')
    if 'key' not in params:
        params['key'] = random.choice(settings['api_keys'])
    if 'page-size' not in params:
        params['page-size'] = 5000
    if 'page-number' not in params:
        params['page-number'] = 0

    data = await get_json(f'{settings["api_url"]}/{url}/', params)

    result_data = data
    while (data.get('data', {}).get('pagination') or {}).get('has_more'):
        params['page-number'] += 1
        data = await get_json(f'{settings["api_url"]}/{url}/', params)
        result_data['data']['items'].extend(data['data']['items'])
        print('downloaded page', params['page-number'])
    return data


async def get_block_range_by_dates(settings, start_date: datetime.date, end_date: datetime.date):
    path = Path(f'blocks/{start_date}_{end_date}.json')
    if path.exists():
        data = json.loads(path.read_text())
    else:
        data = await get_cqt_request(settings, f'block_v2/{start_date}/{end_date}')
        path.write_text(json.dumps(data, indent=2))
    return {
        'starting-block': data['data']['items'][0]['height'],
        'ending-block': data['data']['items'][-1]['height']
    }

async def get_transaction_info(settings, tx_id):
    global TOTAL
    params = {'quote-currency': 'USD', 'format': 'JSON', 'no-logs': False}
    tx_path = Path(f'transactions/{tx_id[:5]}/{tx_id[5:10]}/{tx_id[10:15]}/{tx_id}.json')
    if tx_path.exists():
        TOTAL += 1
        print('exists', tx_id, TOTAL)
        return json.loads(tx_path.read_text())
    async with API_LIMITER:
        data = await get_cqt_request(settings, f'transaction_v2/{tx_id}', params)
        tx_path.parent.mkdir(exist_ok=True, parents=True)
        tx_path.write_text(json.dumps(data, indent=2))
        print('download', tx_id, TOTAL)
        TOTAL += 1
    return data


async def main():
    settings = get_settings()
    lvt_v1 = settings['lvt_v1_contract']
    lvt_v2 = settings['lvt_v2_contract']

    for contract in (lvt_v1, lvt_v2):
        Path(f'contracts/{contract}').mkdir(exist_ok=True)

        # for f in Path(f'contracts/{contract}').glob('*.json'):
        #     f.unlink()

    # for f in Path('blocks').glob('*.json'):
    #     f.unlink()

    web3 = Web3()

    tx_ids = set()
    for contract in (lvt_v1, lvt_v2):
        today_date = datetime.now(timezone.utc).date()
        start_date = datetime(year=2021, month=12, day=12).date()
        total_txs = 0
        while start_date < today_date:
            end_date = start_date + timedelta(days=1)
            blocks = await get_block_range_by_dates(settings, start_date, end_date)
            start_date += timedelta(days=1)

            contract_path = Path(f'contracts/{contract}/{start_date}_{end_date}.json')
            if not contract_path.exists():
                txs = await get_cqt_request(settings, f'events/address/{contract}', blocks)
                contract_path.write_text(json.dumps(txs, indent=2))
            else:
                txs = json.loads(contract_path.read_text())

            total_txs += len(txs['data']['items'])
            for tx in txs['data']['items']:
                tx_ids.add(tx['tx_hash'])
            # print('start_date', start_date, 'total', total_txs, 'v1' if contract == lvt_v1 else 'v2')
    
    # await asyncio.gather(*[get_transaction_info(settings, tx_id) for tx_id in tx_ids])
    # return
    tx_id = random.choice(list(tx_ids))
    # tx_id = '0x27c1180c2ad83721190ec944a3ad3fe66ff327fd6a5fa27940d5091ae4f0922b'
    tx = await get_transaction_info(settings, tx_id)
    # account_address = tx['data']['items'][0]['from_address']
    account_address = '0xe18b1213966DCBF381269b6e10e29c97B933902F'
    #account_address = '0xb3a222bde32c7f77aec8f4ea780e076b33739ffa'
    print(account_address, 'addr')

    account_path = Path(f'accounts/{account_address}.json')
    account_path.parent.mkdir(parents=True, exist_ok=True)
    if not account_path.exists():
        txs = await get_cqt_request(settings, f'/address/{account_address}/transactions_v2')
        account_path.write_text(json.dumps(txs, indent=2))
    else:
        txs = json.loads(account_path.read_text())

    joe_txs = {}
    for tx in (t for t in txs['data']['items'] if t['successful']):
        # print(tx)
        has_lvt = False
        has_joe_tx = False
        filter_tx = False
        for log in tx['log_events']:
            if log['sender_name'] and 'Louverture' in log['sender_name']:
                has_lvt = True
            elif log['sender_name'] and 'Joe LP Token' in log['sender_name']:
                has_joe_tx = True
            for param in ((log.get('decoded') or {}).get('params') or []):
                if param['value'] == '0x051d6213f7186f31f40f3e2ac9e7c8c682fdf1c9':
                    filter_tx = True
        if has_lvt and has_joe_tx and not filter_tx: 
            joe_txs[tx['tx_hash']] = tx['log_events']
        # print('total', len(txs['data']['items']))
    print(len(joe_txs))
    filtered_joe_txs = {}
    for tx, logs in joe_txs.items():
        for log in sorted(logs, key=lambda v: v['log_offset']):
            if (log.get('decoded') or {}).get('name', '') != 'Transfer':
                continue
            val = next((l for l in log.get('decoded', {}).get('params', []) if l['type'] == 'uint256'), None)
            if not val or not val['value']:
                continue
            if log.get('sender_contract_ticker_symbol', '') not in ('WAVAX', 'LVT'):
                continue
            filtered_joe_txs.setdefault(tx, []).append(
                [log['sender_contract_ticker_symbol'], web3.fromWei(int(val['value']), 'Ether') if val else None]
            )

    buys = {}
    sells = {}
    for tx, logs in filtered_joe_txs.items():
        buy_coin = logs[0][0]
        buys.setdefault(buy_coin, 0)
        buys[buy_coin] += logs[0][1]

        sell_coin = logs[1][0]
        sells.setdefault(sell_coin, 0)
        sells[sell_coin] += logs[1][1]
        if sell_coin == 'LVT':
            continue
        print(tx)
        for log in logs:
            print(log)
            print('-----')
        print('*******')

    print('totaly buys', buys['WAVAX'], 'total sells', sells['WAVAX'], 'loss', sells['WAVAX'] - buys['WAVAX'])

if __name__ == '__main__':
    asyncio.run(main())

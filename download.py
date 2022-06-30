import asyncio
import json
import multiprocessing
import time
import pickle

from decimal import Decimal
from datetime import datetime, timezone, timedelta
from pathlib import Path

import httpx
import hashlib
import orjson
import aiolimiter

from web3 import Web3

API_LIMITER = aiolimiter.AsyncLimiter(3, 2)
TOTAL = 0

def get_settings(path: Path = Path('settings.json')) -> dict:
    return json.loads(path.read_text())


async def get_json(url: str, params: dict, headers: dict = None):
    data = None
    retries = 0
    hash_params = params.copy()
    hash_params.pop('key', '')
    url_path = Path(f'urls/{hashlib.sha1((url+str(hash_params)).encode("utf-8")).hexdigest()}.json')
    url_path.parent.mkdir(exist_ok=True, parents=True)
    if url_path.exists():
        return json.loads(url_path.read_text())
    while not data:
        try:
            async with httpx.AsyncClient(timeout=120, http2=True) as cl:
                response = await cl.get(url, params=params, headers=headers)
                data = response.json()
        except Exception as exc:
            print('error', retries, str(exc), url, params)
            retries += 1
            if retries == 20:
                raise
            await asyncio.sleep(5)
    if data['data']:
        url_path.write_text(json.dumps(data, indent=2))
    return data


async def get_cqt_request(settings: dict, url, params: dict = None):
    params = params or {}
    url = url.lstrip('/').rstrip('/')
    if 'key' not in params:
        params['key'] = settings['api_keys'][4]
    if 'page-size' not in params:
        params['page-size'] = 5000
    if 'page-number' not in params:
        params['page-number'] = 0

    data = await get_json(f'{settings["api_url"]}/{url}/', params)
    result_data = data
    old_result_data = None
    pagination = ((data.get('data') or {}).get('pagination') or {})
    while pagination.get('has_more'):
        params['page-number'] += 1
        data = await get_json(f'{settings["api_url"]}/{url}/', params)
        if str(old_result_data) == str(data['data']['items']):
            break
        result_data['data']['items'].extend(data['data']['items'])
        old_result_data = data['data']['items']
        print(url, params)
    return result_data


async def get_block_range_by_dates(settings, start_date: datetime.date, end_date: datetime.date):
    path = Path(f'blocks/{start_date}_{end_date}.json')
    if path.exists():
        data = json.loads(path.read_text())
    else:
        data = await get_cqt_request(settings, f'block_v2/{start_date}/{end_date}')
        path.write_text(json.dumps(data, indent=2))
    if data['data']['items']:
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
    contracts = [c.lower() for c in settings['contracts']]

    for contract in settings['contracts']:
        Path(f'contracts/{contract}').mkdir(exist_ok=True, parents=True)

    tx_ids = set()
    for contract in contracts:
        today_date = datetime.now(timezone.utc).date() - timedelta(days=2)
        start_date = datetime(year=2021, month=12, day=12).date()
        while start_date < today_date:
            end_date = start_date + timedelta(days=1)
            blocks = await get_block_range_by_dates(settings, start_date, end_date)
            start_date += timedelta(days=1)
            if not blocks:
                continue

            contract_path = Path(f'contracts/{contract}/{start_date}_{end_date}.json')
            contract_path.parent.mkdir(exist_ok=True)
            if not contract_path.exists():
                txs = await get_cqt_request(settings, f'events/address/{contract}', blocks)
                contract_path.write_text(json.dumps(txs, indent=2))
            else:
                txs = json.loads(contract_path.read_text())
            
            print('process', start_date, len(txs['data']['items']))
            
            for tx in txs['data']['items']:
                tx_ids.add(tx['tx_hash'])
            print('total txs', len(tx_ids), contract)

    list_path = Path('accounts/list.pickle')
    if list_path.exists():
        accs = pickle.loads(list_path.read_bytes())
    else:
        txs_info = await asyncio.gather(*[get_transaction_info(settings, tx_id) for tx_id in tx_ids])
        accs = set(tx['data']['items'][0]['from_address'] for tx in txs_info)
        list_path.write_bytes(pickle.dumps(accs))

    # contracts 
    accs.remove('0x051d6213f7186f31f40f3e2ac9e7c8c682fdf1c9')
    print('total accounts', len(accs))

    accounts = {}
    accs_path = Path('accounts/data.pickle')
    if accs_path.exists():
        accounts = pickle.loads(accs_path.read_bytes())
    else:
        start = time.time()
        with multiprocessing.Pool(12) as p:
            results = p.map(calc_account_buys_sells, list(accs), 1000)
        end = time.time() - start
        print('time took to process', int(end), 'sec')

        for result in results:
            accounts.update(result)
        accs_path.write_bytes(pickle.dumps(accounts))

    CONTRACTS = [
        '0x482b272af360fdfbf2d6f9b688ce949ae6adc117',
        '0xc7198437980c041c805a1edcba50c1ce5db95118',
        '0x60ae616a2155ee3d9a68541ba4544862310933d4',
        '0x051d6213f7186f31f40f3e2ac9e7c8c682fdf1c9',
        '0x000000000000000000000000000000000000dead',
        '0x1f0879807b88e42f165ff9f9d9aaf48d5ab0634f',
        '0x726ac44f01672206389dbab09c5e4fd3099ecad2',
    ]
    moved_addrs = set()
    not_found_moved_addrs = set()
    for addr, data in accounts.items():
        for c in CONTRACTS:
            if c in data['moved']:
                data['moved'] = {}
            #data['moved'].pop(c, None)

        if data['moved']:
            for maddr in data['moved']:
                if maddr not in accounts:
                   not_found_moved_addrs.add(maddr) 
                moved_addrs.add(maddr)

        else:
            continue

    if not_found_moved_addrs:
        with multiprocessing.Pool(12) as p:
            results = p.map(calc_account_buys_sells, list(not_found_moved_addrs), 500)
        for result in results:
            accounts.update(result)
        accs_path.write_bytes(pickle.dumps(accounts))
    
    total_accounts_profited = 0
    total_accounts_loss = 0
    total_loss = 0
    total_avax_loss = 0
    total_merged = 0

    accounts.pop('0xe366fc3f537d1eb9119a49e152d78b5fd005589a')
    accounts.pop('0x024b938af25ed40e515d87366ba4cf95d38a826e')
    accounts.pop('0xf0634fb057058e350f43a10872c88818e96c0aec')
    accounts.pop('0x95efdb2c40349c9a47ca0e0b3fe15d61b4c62cb2')

    for addr, data in accounts.items():
        # if addr != '0xf0634fb057058e350f43a10872c88818e96c0aec':
        #     continue
        if addr in moved_addrs:
            continue

        moved = list(data['moved'].keys())
        process_moved = set(moved)
        while moved:
            total_merged += 1
            maddr = moved.pop()
            # print('addr', addr, 'moved ->', maddr)
            moved_data = accounts[maddr]
            for nmaddr in moved_data['moved']:
                if nmaddr not in moved and nmaddr not in process_moved:
                    moved.append(nmaddr)
                    process_moved.add(nmaddr) 
            if 'usd_diff' not in moved_data:
                total_moved_no_merge += 1
                continue

            data['usd_diff'] += moved_data['usd_diff']
            data['diff'] += moved_data['diff']

        if data['usd_diff'] > 0:
            total_accounts_profited += 1
            # print(addr, 'PROFIT', round(data['usd_diff'], 3))
        elif data['usd_diff'] < 0 and data['buys']['WAVAX']:
            total_accounts_loss += 1
            total_loss += data['usd_diff'] 
            total_avax_loss += data['diff']
            # print(addr, 'LOSS', round(data['usd_diff'], 3))

    print('total profited', total_accounts_profited)
    print('total in loss', total_accounts_loss)
    print('total loss $', round(total_loss, 4))
    print('total avax loss', round(total_avax_loss, 4))
    print('total merged (multiple accs)', total_merged)
    print('total break even or inactive', len(accounts) - total_accounts_loss - total_accounts_profited)


def calc_account_buys_sells(account_address):
    web3 = Web3()
    settings = get_settings()
    account_path = Path(f'accounts/{account_address}.json')
    account_path.parent.mkdir(parents=True, exist_ok=True)
    if not account_path.exists():
        print('downloading txs', account_address, )
        txs = asyncio.run(get_cqt_request(settings, f'/address/{account_address}/transactions_v2'))
        account_path.write_text(json.dumps(txs, indent=2))
    else:
        # print('ready txs', account_address)
        txs = orjson.loads(account_path.read_text())

    joe_txs = {}
    move_txs = {}
    for tx in (t for t in txs['data']['items'] if t['successful']):
        has_lvt = False
        has_joe_tx = False
        filter_tx = False

        if len(tx['log_events']) == 1:
            try:
                if tx['log_events'][0]['decoded']['name'] == 'Transfer' and tx['log_events'][0]['sender_contract_ticker_symbol'] == 'LVT':
                    p = [p['value'] for p in tx['log_events'][0]['decoded']['params'] if p['name'] == 'to'][0]
                    if p not in {'0xff579d6259dedcc80488c9b89d2820bcb5609160', '0xd641e62525e830e98cb9d7d033a538a1f092ff34', account_address}:
                        move_txs.setdefault(p, set()).add(tx['tx_hash'])
            except (TypeError, KeyError):
                pass

        for log in tx['log_events']:
            if log['sender_name'] and 'Louverture' in log['sender_name'] and log['sender_address']:
                has_lvt = True
            elif log['sender_name'] and 'Joe LP Token' in log['sender_name']:
                has_joe_tx = True
            for param in ((log.get('decoded') or {}).get('params') or []):
                if param['value'] == '0x051d6213f7186f31f40f3e2ac9e7c8c682fdf1c9':
                    filter_tx = True
        if has_lvt and has_joe_tx and not filter_tx: 
            joe_txs[tx['tx_hash']] = tx['log_events']

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
                [log['sender_contract_ticker_symbol'], web3.fromWei(int(val['value']), 'Ether'), tx]
            )

    buys = {'WAVAX': 0, 'LVT': 0, 'WAVAX_txs': {}, 'LVT_txs': {}}
    sells = {'WAVAX': 0, 'LVT': 0, 'WAVAX_txs': {}, 'LVT_txs': {}}
    for tx, logs in filtered_joe_txs.items():
        if len(logs) < 2:
            # print(account_address, 'smth bad with tx', tx)
            continue
        buy_coin = logs[0][0]
        buys[buy_coin] += logs[0][1]
        buys[f'{buy_coin}_txs'][logs[0][2]] = logs[0][1]

        sell_coin = logs[1][0]
        sells[sell_coin] += logs[1][1]
        sells[f'{sell_coin}_txs'][logs[1][2]] = logs[1][1]

    WAVAX = '0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7'.lower()
    txs = (t for t in txs['data']['items'] if t['successful'])
    txs = {t['tx_hash']: t for t in txs}
    buys['USD'] = Decimal('0')
    for tx, value in buys['WAVAX_txs'].items():
        dd = txs[tx]['log_events'][0]['block_signed_at'].split('T')[0]
        resp = asyncio.run(get_json(f'https://api.covalenthq.com/v1/pricing/historical_by_addresses_v2/43114/USD/{WAVAX}/', {'key': settings["api_keys"][0], 'from': dd, 'to': dd}))
        price = resp['data'][0]['prices'][0]['price']
        # print(tx, dd, round(value, 3), 'AVAX', round(Decimal(price) * value, 3), 'USD', 'AVAX price', round(price, 3))
        buys['USD'] += Decimal(price) * value
    
    # print('total USD spent', abs(total_diff))
    # print('-'*10)
    
    # print('total AVAX got back by selling LVT', data['sells']['WAVAX'])
    sells['USD'] = Decimal('0')
    for tx, value in sells['WAVAX_txs'].items():
        dd = txs[tx]['log_events'][0]['block_signed_at'].split('T')[0]
        resp = asyncio.run(get_json(f'https://api.covalenthq.com/v1/pricing/historical_by_addresses_v2/43114/USD/{WAVAX}/', {'key': settings["api_keys"][0], 'from': dd, 'to': dd}))
        price = resp['data'][0]['prices'][0]['price']
        # print(tx, dd, round(value, 3), 'AVAX', round(Decimal(price) * value, 3), 'USD', 'AVAX price', round(price, 3))
        sells['USD'] += Decimal(price) * value

    return {
        account_address: {
            'moved': move_txs, 'buys': buys,
            'sells': sells, 'diff': sells['WAVAX'] - buys['WAVAX'],
            'usd_diff': sells['USD'] - buys['USD']
        }
    }

if __name__ == '__main__':
    asyncio.run(main())
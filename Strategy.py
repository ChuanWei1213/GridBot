import numpy as np
import math
import pandas as pd
from datetime import datetime, timedelta
import time
from pybit.unified_trading import HTTP
import json
import os
from pathlib import Path
from requests.exceptions import ReadTimeout, ConnectTimeout, ConnectionError
from urllib3.exceptions import ProtocolError
from pybit.exceptions import InvalidRequestError
from itertools import repeat
from abc import abstractmethod

# Logging setup: rotating file and stream handlers
import logging
from logging.handlers import RotatingFileHandler

MAX_ORDER_PER_BATCH_PLACE = 10
LIMIT_PER_PAGE = 50
SEC_LIMIT_PER_POST = .1
SEC_LIMIT_PER_GET = .02


class FlushRotatingFileHandler(RotatingFileHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()


class Strategy:
    """
    Dunder methods (double underscore methods) or magic methods
    """
    def __init__(self, api_key, api_secret, demo):
        if type(self) is Strategy:
            raise TypeError("Strategy is an abstract class and cannot be instantiated directly.")
        self.api_key = api_key
        self.api_secret = api_secret
        self.demo = demo
        
        self.session = HTTP(
            api_key=api_key,
            api_secret=api_secret,
            demo=demo,
        )


    def __repr__(self, indent=4):
        pad = ' ' * indent
        return (
            f'{self.class_name}(\n' + 
            ', \n'.join(
                f'{pad}{k}={repr(v)}' for k, v in self.params.items()
            ) + 
            f'\n)'
        )
        
    """
    Properties
    """
    @property
    @abstractmethod
    def params(self) -> dict:
        raise NotImplementedError('Subclasses must implement params()')
    
    
    @property
    @abstractmethod
    def state_params(self) -> dict:
        raise NotImplementedError('Subclasses must implement state_params()')
    
    
    @property
    @abstractmethod
    def base_path(self) -> str:
        raise NotImplementedError('Subclasses must implement base_path()')
        
        
    @property
    def class_name(self):
        return self.__class__.__name__
    
    
    @property
    @abstractmethod
    def id(self):
        raise NotImplementedError('Subclasses must implement id()')
        
        
    """
    Helper methods
    """
    @staticmethod
    def _check_time(start_time, end_time):
        if start_time > end_time:
            raise ValueError('Start time must be less than end time')
        if end_time < datetime.now():
            raise ValueError('End time must be greater than current time')
        
    
    @staticmethod
    def _get_order_desc(order):
        trigger_desc = f'Trigger at {order["triggerPrice"]}' if ('triggerPrice' in order and order['triggerPrice']) else ''
        price = order['avgPrice'] if ('avgPrice' in order and order['avgPrice']) \
                else order['price'] if ('price' in order and order['price']) \
                else 'Market'
        trade_desc = f'{order["side"]} {order["qty"]} at {price}'
        return f'{trigger_desc}; {trade_desc}' if trigger_desc else trade_desc
    
    
    @staticmethod
    def _create_orders(raw_orders):
        # Create a copy of the orders to avoid modifying the original
        orders = [raw_order.copy() for raw_order in raw_orders]
        # Add timestamp to orderLinkId to make it unique
        for order in orders:
            order['orderLinkId'] += datetime.now().strftime('%m%d%H%M%S%f')[:-3]
            
        return orders
        
        
    """
    Pre-run methods
    """
    def _set_logger(self, level: str | int):
        parents = Path(f'./log/{self.base_path}')
        if not parents.exists():
            parents.mkdir(parents=True, exist_ok=True)
            
        self.log_file_path = Path(f'{parents}/{self.id}.log')
        
        logger = logging.getLogger(self.id)
        logger.setLevel(level)

        _fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        _file_handler = FlushRotatingFileHandler(
            self.log_file_path,
            maxBytes=1_000_000,  # 1 MB per file
            backupCount=5        # keep the five most‑recent log files
        )
        _file_handler.setFormatter(_fmt)
        logger.addHandler(_file_handler)

        _stream_handler = logging.StreamHandler()
        _stream_handler.setFormatter(_fmt)
        logger.addHandler(_stream_handler)
        
        self.logger = logger
        self.logger.info(f'\n{self.__repr__()}')

    def _load_state(self):
        """Load active order IDs from disk (if any)."""
        path = Path(f'./state/{self.base_path}/{self.id}.json')
        if path.exists():
            with open(path, 'r') as fp:
                data = json.load(fp)

            for k, v in data.items():
                setattr(self, k, v)
                
            if self.status == 'Closed':
                raise AssertionError(f'{self.id} is already closed')
            if self.status != 'Disconnected':
                raise AssertionError(f'{self.id} is already running')
            
            self.logger.info(
                f'Loaded state from {path}: '
            )
            return True
         
        return False


    def _save_state(self):
        """Persist active order IDs for the next run."""
        parents = Path(f'./state/{self.base_path}')
        if not parents.exists():
            parents.mkdir(parents=True, exist_ok=True)
            
        self.state_file_path = Path(f'{parents}/{self.id}.json')
        with open(self.state_file_path, 'w') as fp:
            json.dump(
                self.state_params,
                fp,
                indent=4
            )
        
        
    def _waiting(self, start_time):
        if start_time < datetime.now():
            self.logger.warning(f'Start time has passed')
            return
        self.logger.info(f'Waiting for start time: {start_time}')
        self.status = 'Waiting'
        while datetime.now() < start_time:
            time.sleep(1)
        self.logger.info(f'Start time reached')
    

    """
    Get methods
    """
    def _check_leverage(self, category, symbol, leverage):
        instruments_info = self.session.get_instruments_info(
            category=category,
            symbol=symbol,
        )['result']['list'][0]
        leverageFilter = instruments_info['leverageFilter']
        minLeverage = float(leverageFilter['minLeverage'])
        maxLeverage = float(leverageFilter['maxLeverage'])
        if leverage < minLeverage or leverage > maxLeverage:
            raise ValueError(f'Leverage must be between {minLeverage} and {maxLeverage}')
        
        
    def _check_balance(self, coin, total_investment):
        if total_investment <= 0:
            raise ValueError('Total investment must be greater than 0')
        balance = float(self.session.get_wallet_balance(
            accountType='UNIFIED',
            coin=coin,
        )['result']['list'][0]['coin'][0]['equity'])
        if total_investment > balance:
            raise ValueError(f'Not enough balance. Available: {balance}, Required: {total_investment}')
        
        
    def _get_instruments_info(self, category, symbol):
        r = self.session.get_instruments_info(
            category=category,
            symbol=symbol,
        )
        if r['retCode'] != 0:
            raise ValueError(f'Error getting instruments info: {r["retMsg"]}')
        
        return r['result']['list'][0]
    
    
    def _get_risk_limit(self, category, symbol):
        r = self.session.get_risk_limit(
            category=category,
            symbol=symbol,
        )
        if r['retCode'] != 0:
            raise ValueError(f'Error getting risk limit: {r["retMsg"]}')
        
        return r['result']['list'][0]
    

    def _get_open_order(self, category, symbol, id):
        r = self.session.get_open_orders(
            category=category,
            symbol=symbol,
            orderLinkId=id
        )
        if r['retCode'] != 0:
            self.logger.debug(f'Error getting order {id}: {r["retMsg"]}')
            return None
        if not r['result']['list']:
            self.logger.debug(f'Order {id} not found')
            return None
        return r['result']['list'][0]
    
    
    def _get_all_active_orders(self, category, symbol, n):
        nextPageCursor = None
        orders = []
        for _ in range(0, n, LIMIT_PER_PAGE):
            r = self.session.get_open_orders(
                category=category,
                symbol=symbol,
                orderFilter='Order',
                limit=LIMIT_PER_PAGE,
                cursor=nextPageCursor,
            )
            nextPageCursor = r['result']['nextPageCursor']
            orders += r['result']['list']
            
        return orders
    
    """
    Post methods
    """
    def _to_maxlev(self, category, symbol):
        """
        Set to max leverage
        """
        current_leverage = self.session.get_positions(
            category=category,
            symbol=symbol,
        )['result']['list'][0]['leverage']

        leverageFilter = self._get_instruments_info(category, symbol)['leverageFilter']
        maxLeverage = float(leverageFilter['maxLeverage'])

        if float(current_leverage) != maxLeverage:
            r = self.session.set_leverage(
                category=category,
                symbol=symbol,
                buyLeverage=str(maxLeverage),
                sellLeverage=str(maxLeverage),
            )
            if r['retCode'] != 0:
                raise ValueError(f'Error setting leverage: {r["retMsg"]}')


    def _to_cross_margin(self):
        """
        Set to cross margin
        """
        r = self.session.set_margin_mode(
            setMarginMode="REGULAR_MARGIN",
        )
        assert r['retCode'] == 0, \
            f'Error setting margin mode: {r["result"]["reasons"][0]["reasonMsg"]}'
            
            
    def _cancel_all_orders(self, category, symbol):
        r = self.session.cancel_all_orders(
            category=category,
            symbol=symbol,
        )
        if r['retCode'] == 0:
            self.logger.info(f'Cancelled all open orders')
        else:
            self.logger.warning(f'Error cancelling open orders: {r["retMsg"]}')
            
            
    def _close_position(self, category, symbol):
        position = self.session.get_positions(
            category=category,
            symbol=symbol,
        )['result']['list'][0]

        close_order = self._create_orders([{
            'symbol': symbol,
            'side': 'Buy' if position['side'] == 'Sell' else 'Sell',
            'orderType': 'Market',
            'qty': position['size'],
            'orderLinkId': f'{self.id}-close-',
            'reduceOnly': True,
        }])[0]
        if position['side']:
            Strategy._place_order(
                self=self,
                category=category,
                order=close_order
            )
            start = time.perf_counter()
            for i in range(100):
                order = self._get_open_order(category, symbol, close_order['orderLinkId'])
                if order is not None:
                    self.logger.debug(f'Order filled after {time.perf_counter() - start:.2f} seconds. Iteration: {i+1}')
                    desc = self._get_order_desc(order)
                    self.logger.info(f'{desc} to close position')
                    break
                if i == 99:
                    self.logger.warning(f'Error closing position: {order["orderStatus"]}')


    def _place_orders(self, category, orders):
        ids = [''] * len(orders)
        for i in range(0, len(orders), MAX_ORDER_PER_BATCH_PLACE):
            batch = orders[i:i+MAX_ORDER_PER_BATCH_PLACE]
            r = self.session.place_batch_order(
                category=category,
                request=batch,
            )
            if r['retCode'] != 0:
                self.logger.warning(f'Error placing orders: {r["retMsg"]}')
            for j, (result, retExtInfo, order) in \
                enumerate(zip(r['result']['list'], r['retExtInfo']['list'], batch), start=i):
                if retExtInfo['code'] != 0:
                    self.logger.warning(f'Error placing {result["orderLinkId"]}: {retExtInfo["msg"]}')
                else:
                    desc = self._get_order_desc(order)
                    self.logger.info(f'Placed: {result["orderLinkId"]} ({desc})')
                    ids[j] = order['orderLinkId']
                    
            time.sleep(1)
            
        return ids


    def _place_order(self, category, order):
        r = self.session.place_order(
            category=category,
            **order
        )
        if r['retCode'] != 0:
            self.logger.warning(f'Error placing {order["orderLinkId"]}: {r["retExtInfo"]}')
            return ''
        desc = self._get_order_desc(order)
        self.logger.info(f'Placed: {order["orderLinkId"]} ({desc})')
        return order['orderLinkId']


    """"
    Abstract methods
    """
    @abstractmethod
    def _init_orders(self):
        raise NotImplementedError('Subclasses must implement _init_orders()')


    @abstractmethod
    def _main_loop(self):
        raise NotImplementedError('Subclasses must implement _main()')
    
    
    @abstractmethod
    def _setup(self):
        raise NotImplementedError('Subclasses must implement _setup()')
    
    
    @abstractmethod
    def close(self):
        raise NotImplementedError('Subclasses must implement close()')
    
    
    """
    Main methods
    """
    def run(self):
        try:
            if not self._load_state():
                self._setup()
                
            self._run_with_retries()
        except Exception as e:
            self.logger.exception(f'An error occurred: {e}')
            if self.status != 'Closed':
                self.disconnect()

        
    def _run_with_retries(self):
        self.status = 'Running'
        self._save_state()
        self.logger.info('Running...')
        RETRY_QUOTA = 5
        quota = RETRY_QUOTA
        while quota:
            try:
                start = time.time()
                self._main_loop()
                break
            except (ReadTimeout, ConnectTimeout, ConnectionError, ProtocolError) as e:
                if quota < RETRY_QUOTA and time.time() - start > 60:
                    quota = RETRY_QUOTA
                    self.logger.debug(f'Quota reset to {RETRY_QUOTA}')
                quota -= 1
                self.logger.debug(f'{e}. Retrying in 5 seconds... quota: {quota}/{RETRY_QUOTA}')
                time.sleep(5)
            except KeyboardInterrupt:
                command = input('\n1. Close \n2. Disconnect \n3. Continue \nEnter command: ')
                if command == '1':
                    self.logger.info('Closing by user...')
                    break
                elif command == '2':
                    self.logger.info('Disconnected by user')
                    self.disconnect()
                    return
                else:
                    continue

        if quota == 0:
            self.logger.warning(f'Quota exceeded. Check your connection and try again.')
            self.disconnect()
        else:
            self.close()
            
            
    def disconnect(self):
        self.status = 'Disconnected'
        self._save_state()


class GridBot(Strategy):
    def __init__(
        self, 
        api_key, api_secret, demo, 
        start_time, end_time,
        category, symbol, 
        low, high, 
        mode, ngrid, 
        leverage, total_investment, 
        low_sl, high_sl,
        start_price=None,
        update_period=0,
        logger_level=logging.INFO,
    ):
        super().__init__(api_key, api_secret, demo)

        start_time = pd.Timestamp(start_time)
        end_time = pd.Timestamp(end_time)
        
        self._check_time(start_time, end_time)
        
        if (category, symbol) not in [('linear', 'BTCUSDT'), ('inverse', 'BTCUSD')]:
            raise ValueError('(`category`, `symbol`) must be either (linear, BTCUSDT) or (inverse, BTCUSD)')
            
        if low >= high:
            raise ValueError('Low price must be less than high price')
        
        if mode not in ['Arithmetic', 'Geometric']:
            raise ValueError('Mode must be either "Arithmetic" or "Geometric"')
        
        if ngrid < 2 or ngrid > 100:
            raise ValueError('Number of grids must be between 2 and 100')
        
        self._check_leverage(category, symbol, leverage)
        
        # if category == 'linear':
        #     self._check_balance('USDT', total_investment)
        # else:
        #     price = float(self.session.get_tickers(
        #         category=category,
        #         symbol=symbol,
        #     )['result']['list'][0]['markPrice'])
        #     self._check_balance('BTC', total_investment / price)
        
        if high_sl < high or low_sl > low:
            raise ValueError(f'Stoploss must be within the grid levels.')
        
        if update_period < 0:
            raise ValueError('Update period must be greater than or equal to 0')
        
        if not demo:
            # Bybit sets the following condition as 'not enough profit per grid'
            MIN_GRID_SPREAD = 0.0025
            mean = (high + low) / 2
            half_spread = high / mean - 1
            if half_spread * 2 / ngrid < MIN_GRID_SPREAD:
                raise ValueError('Not enough profit per grid')
        
        self.start_time = start_time
        self.end_time = end_time
        self.category = category
        self.symbol = symbol
        self.low = low
        self.high = high
        self.mode = mode
        self.ngrid = ngrid
        self.leverage = leverage
        self.total_investment = total_investment
        self.low_sl = low_sl
        self.high_sl = high_sl
        self.start_price = start_price
        self.update_period = update_period
        self.logger_level = logger_level if isinstance(logger_level, str) else logging._levelToName[logger_level]
        
        self.status = 'Waiting'
        self._set_logger(logger_level)

    @property
    def params(self):
        return {
            'api_key': 'YOUR_API_KEY',
            'api_secret': 'YOUR_API_SECRET',
            'demo': self.demo,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'category': self.category,
            'symbol': self.symbol,
            'low': self.low,
            'high': self.high,
            'mode': self.mode,
            'ngrid': self.ngrid,
            'leverage': self.leverage,
            'total_investment': self.total_investment,
            'low_sl': self.low_sl,
            'high_sl': self.high_sl,
            'start_price': self.start_price,
            'update_period': self.update_period,
            'logger_level': self.logger_level,
        }


    @property
    def state_params(self):
        return {
            'active_long_ids': self.active_long_ids,
            'active_short_ids': self.active_short_ids,
            'active_sl_ids': self.active_sl_ids,
            'start_price': self.start_price,
            'status': self.status,
        }


    @property
    def id(self):
        if hasattr(self, '_id'):
            return self._id
        
        mean = (self.high + self.low) / 2
        spread = round(self.high / mean - 1, 3)
        self._id = f'{self.start_time.strftime("%y%m%d%H%M%S")}_{self.ngrid}_{spread}'
        return self._id
    
    
    @property
    def base_path(self):
        if hasattr(self, '_base_path'):
            return self._base_path
        
        self._base_path = Path(f'{self.class_name}{"/test" if self.demo else ""}/{self.symbol}')
        return self._base_path

  
    @property
    def instruments_info(self):
        if hasattr(self, '_instruments_info'):
            return self._instruments_info
        
        self._instruments_info = self._get_instruments_info(self.category, self.symbol)
        return self._instruments_info
    

    @property
    def risk_limit(self):
        if hasattr(self, '_risk_limit'):
            return self._risk_limit
        
        self._risk_limit = self._get_risk_limit(self.category, self.symbol)
        return self._risk_limit
    

    @property
    def INITIAL_MARGIN(self):
        if hasattr(self, '_INITIAL_MARGIN'):
            return self._INITIAL_MARGIN
        
        self._INITIAL_MARGIN = float(self.risk_limit['initialMargin'])
        return self._INITIAL_MARGIN
    
    
    @property
    def MAINTENANCE_MARGIN(self):
        if hasattr(self, '_MAINTENANCE_MARGIN'):
            return self._MAINTENANCE_MARGIN
        
        self._MAINTENANCE_MARGIN = float(self.risk_limit['maintenanceMargin'])
        return self._MAINTENANCE_MARGIN
    
        
    @property
    def grids(self):
        if hasattr(self, '_grids'):
            return self._grids
        
        tickSize = float(self.instruments_info['priceFilter']['tickSize'])
        decimal = int(-math.log10(tickSize))

        ## Grid levels based on mode
        if self.mode == 'Arithmetic':
            self._grids = np.linspace(self.low, self.high, self.ngrid + 1).round(decimal)
        else:
            self._grids = np.geomspace(self.low, self.high, self.ngrid + 1).round(decimal)
            
        return self._grids
    

    @property
    def mid_idx(self):
        if hasattr(self, '_mid_idx'):
            return self._mid_idx
        
        if self.start_price is None:
            current_price = self.session.get_tickers(
                category=self.category,
                symbol=self.symbol,
            )['result']['list'][0]['markPrice']
            self.start_price = float(current_price)
        
        # Find index of grid level closest to current price
        # Choose the last index in case of ties
        tickSize = float(self.instruments_info['priceFilter']['tickSize'])
        decimal = int(-math.log10(tickSize))
        self._mid_idx = self.grids.shape[0] - 1 - np.abs(self.grids - self.start_price)[::-1].round(decimal).argmin()
        return self._mid_idx
    

    @property
    def qty(self):
        if hasattr(self, '_qty'):
            return self._qty
        
        grids = self.grids
        mid_idx = self.mid_idx
        
        larger_portion = grids[:mid_idx] if mid_idx > self.ngrid - mid_idx else grids[mid_idx+1:]
        if self.category == 'linear':
            qty = self.total_investment * self.leverage \
                / (grids.sum() * (1 + self.INITIAL_MARGIN) + larger_portion.sum() * self.MAINTENANCE_MARGIN)
        else:
            qty = self.total_investment * self.leverage \
                / (grids.size * (1 + self.INITIAL_MARGIN) + larger_portion.size * self.MAINTENANCE_MARGIN)
                        

        minOrderQty = float(self.instruments_info['lotSizeFilter']['minOrderQty'])
        if qty < minOrderQty:
            raise ValueError(f'Not enough investventment to open position. ')

        qtyStep = float(self.instruments_info['lotSizeFilter']['qtyStep'])
        qty = math.floor((qty - minOrderQty) / qtyStep) * qtyStep + minOrderQty
        self._qty = round(qty, int(-math.log10(qtyStep)))
        return self._qty


    @property
    def raw_orders(self):
        if hasattr(self, '_raw_orders'):
            return self._raw_orders
        
        grids = self.grids
        qty = self.qty
        mid_idx = self.mid_idx
        
        self._raw_orders = {}
        
        # Open orders
        self._raw_orders['op'] = [
            {
                'symbol': self.symbol,
                'side': 'Buy',
                'orderType': 'Limit',
                'qty': str(qty),
                'price': str(price),
                'orderLinkId': f'{self.id}-op-{i}-'
            }
            for i, price in enumerate(grids[:mid_idx])
        ] + [None] + [
            {
                'symbol': self.symbol,
                'side': 'Sell',
                'orderType': 'Limit',
                'qty': str(qty),
                'price': str(price),
                'orderLinkId': f'{self.id}-op-{i}-'
            }
            for i, price in enumerate(grids[mid_idx+1:], start=mid_idx+1)
        ]
        
        # Take profit orders
        self._raw_orders['tp'] = [
            {
                'symbol': self.symbol,
                'side': 'Sell',
                'orderType': 'Limit',
                'qty': str(qty),
                'price':str(price),
                'orderLinkId': f'{self.id}-tp-{i}-',
                # 'reduceOnly': True,
            }
            for i, price in enumerate(grids[1:mid_idx+1])
        ] + [None] + [
            {
                'symbol': self.symbol,
                'side': 'Buy',
                'orderType': 'Limit',
                'qty': str(qty),
                'price': str(price),
                'orderLinkId': f'{self.id}-tp-{i}-',
                # 'reduceOnly': True,
            }
            for i, price in enumerate(grids[mid_idx:-1], start=mid_idx+1)
        ]
        
        # Stop loss orders
        """
        qty=0, reduceOnly=True, closeOnTrigger=True 
            -> close position up to maxMktOrderQty
        """
        self._raw_orders['sl'] = [
            {
                'symbol': self.symbol,
                'side': 'Sell',
                'orderType': 'Market',
                'qty': '0',
                'triggerDirection': 2,
                'triggerPrice': str(self.low_sl),
                'triggerBy': 'MarkPrice',
                'orderLinkId': f'{self.id}-sl-0-',
                'reduceOnly': True,
                'closeOnTrigger': True,
            },
            {
                'symbol': self.symbol,
                'side': 'Buy',
                'orderType': 'Market',
                'qty': '0',
                'triggerDirection': 1,
                'triggerPrice': str(self.high_sl),
                'triggerBy': 'MarkPrice',
                'orderLinkId': f'{self.id}-sl-1-',
                'reduceOnly': True,
                'closeOnTrigger': True,
            }
        ]
        
        return self._raw_orders
    
    
    @staticmethod
    def rev_order_type(order_type):
        return 'tp' if order_type == 'op' else 'op'
        

    def _place_orders(self, order_types, indices):
        raw_orders = [self.raw_orders[order_type][idx] for order_type, idx in zip(order_types, indices)]
        orders = self._create_orders(raw_orders)
        return super()._place_orders(self.category, orders)
    

    def _place_order(self, order_type, idx, no_duplicate=False):
        """
        Place an order by type and index.
        If the order already exists, return its orderLinkId without placing a new order.
        """
        raw_order = self.raw_orders[order_type][idx]
        if no_duplicate:
            active_orders = self._get_all_active_orders(self.category, self.symbol, self.ngrid)
            if raw_order['price'] in [o['price'] for o in active_orders]:
                return next(o['orderLinkId'] for o in active_orders if o['price'] == raw_order['price'])
        order = self._create_orders([raw_order])[0]
        return super()._place_order(self.category, order)
    

    def _place_init_orders(self):
        mid_idx = self.mid_idx
        """
        Use stack to efficiently modify the active orders
        """
        self.active_long_ids = self._place_orders(repeat('op'), range(mid_idx))
        self.active_short_ids = self._place_orders(repeat('op'), range(self.ngrid, mid_idx, -1))
        self.active_sl_ids = self._place_orders(repeat('sl'), range(2))
        ids = self.active_long_ids + self.active_short_ids + self.active_sl_ids
        return all(ids)
        

    def _setup(self):
        # No open orders
        orders = self.session.get_open_orders(
            category=self.category,
            symbol=self.symbol,
        )['result']['list']
        if orders:
            raise AssertionError(f'Found open order(s)')
        
        # No open positions
        position = self.session.get_positions(
            category=self.category,
            symbol=self.symbol,
        )['result']['list'][0]
        if position['side']:
            raise AssertionError('Found open position')
        
        self._to_maxlev(self.category, self.symbol)
        self._to_cross_margin()
        
        self._waiting(self.start_time)
        
        if not self._place_init_orders():
            self.close()
            raise AssertionError(f'Error placing initial orders')
        
        self._save_state()
        
    
    def _main_loop(self):
        prev_update = 0
        while datetime.now() < self.end_time:
            if time.time() - prev_update < self.update_period:
                continue
            
            for i, ids in enumerate([self.active_long_ids, self.active_short_ids]):
                if ids:
                    continue
                order = self._get_open_order(self.category, self.symbol, self.active_sl_ids[i])
                if order and order['orderStatus'] == 'Filled':
                    desc = self._get_order_desc(order)
                    self.logger.info(f'Stop loss triggered ({desc})')
                    return
            
            active_ids = [self.active_long_ids, self.active_short_ids]
            filled_ids = [[], []] # [filled_long_ids, filled_short_ids]
            new_orders = [[], []] # [new_short_orders, new_long_orders]
            for i, ids in enumerate(active_ids):
                while ids:
                    order = self._get_open_order(self.category, self.symbol, ids[-1])
                    if order and order['orderStatus'] == 'Filled':
                        desc = self._get_order_desc(order)
                        self.logger.info(f'Filled: {order["orderLinkId"]} ({desc})')
                        filled_ids[i].append(ids[-1])
                        _, order_type, order_idx, _ = ids[-1].split('-')
                        new_orders[i].append((self.rev_order_type(order_type), int(order_idx)))
                        ids.pop()
                    else:
                        break

            if any(new_orders):
                """
                `new_orders[0] and new_orders[1]` is true only if: 
                    order (e.g. long at 9) filled 
                    -> reverse order (short at 10) not yet placed 
                    -> oppisite order(s) (short at 11) filled
                happens between this and the previous interation.
                Mainly happens when the program stops running for certain amount of time
                In other words, this handles restarting
                Example: 
                    active_orders = [long at [1], short at [10]]
                    filled_orders = [long at [5, 4, 3, 2], short at [7, 8, 9]]
                    new_orders = [short at [6, 5, 4, 3], long at [6, 7, 8]]
                    current_price = 3.5
                    1. 
                        long at 2, short at 9
                        active_orders = [long at [1, 2], short at [10, 9]]
                        filled_orders = [long at [5, 4, 3], short at [7, 8]]
                        new_orders = [short at [6, 5, 4], long at [6, 7]]
                    2. 
                        push unused filled_order back to active_orders, clear unused new_orders
                        active_orders = [long at [1, 2, 3, 4, 5], short at [10, 9]]
                        filled_orders = [long at [], short at [7, 8]]
                        new_orders = [short at [], long at [6, 7]]
                        long at 6, 7
                        active_orders = [long at [1, 2, 3, 4, 5, 6, 7], short at [10, 9]]
                        filled_orders = [long at [], short at []]
                        new_orders = [long at [], short at []]
                    3. (next _main_loop iteration)
                        Since long at 6, 7 are executed immediately,
                        active_orders = [long at [1, 2], short at [10, 9]]
                        filled_orders = [long at [7, 6, 5, 4, 3], short at []]
                        new_orders = [short at [8, 7, 6, 5, 4], long at []]
                    4. 
                        short at 8, 7, 6, 5, 4
                        active_orders = [long at [1, 2], short at [10, 9, 8, 7, 6, 5, 4]]
                        filled_orders = [long at [], short at []]
                        new_orders = [long at [], short at []]
                """
                while new_orders[0] and new_orders[1]:
                    short_order_type, short_order_idx = new_orders[0][-1]
                    long_order_type, long_order_idx = new_orders[1][-1]
                    short_order = self.raw_orders[short_order_type][short_order_idx]
                    long_order = self.raw_orders[long_order_type][long_order_idx]
                    current_price = float(self.session.get_tickers(
                        category=self.category,
                        symbol=self.symbol,
                    )['result']['list'][0]['markPrice'])
                    if float(short_order['price']) <= current_price <= float(long_order['price']):
                        # Place reverse orders to prevent self matching (immediate buy and sell)
                        self.logger.debug(
                            f'Current price: {current_price}\n'
                            f'{short_order_type}-{short_order_idx} ({self._get_order_desc(short_order)}) '
                            f'and {long_order_type}-{long_order_idx} ({self._get_order_desc(long_order)}) '
                            f'are skipped due to self matching.'
                        )
                        long_id = self._place_order(self.rev_order_type(short_order_type), short_order_idx, True)
                        short_id = self._place_order(self.rev_order_type(long_order_type), long_order_idx, True)
                        if long_id and short_id:
                            new_orders[0].pop()
                            new_orders[1].pop()
                        # If for whatever reason only one of the orders is placed,
                        # the following conditions prevent duplicating active ids
                        if long_id != active_ids[0][-1]:
                            active_ids[0].append(long_id)
                        if short_id != active_ids[1][-1]:
                            active_ids[1].append(short_id)
                    else:
                        # If i = 0
                        # Limit long order will be executed immediately
                        # Only place long order for the next main loop to handle this filled order
                        # This prevents pushing other short ids to `self.active_short_ids` before the
                        # corresponding short order is placed
                        # Vice versa for i = 1
                        i = int(current_price > float(long_order['price']))
                        active_ids[i] += filled_ids[i][::-1]
                        filled_ids[i].clear()
                        new_orders[i].clear()
                        
                # Normal cases
                for i in range(2):
                    for j, (order_type, order_idx) in enumerate(new_orders[i]):
                        order_id = self._place_order(order_type, order_idx)
                        if order_id:
                            active_ids[1-i].append(order_id)
                        else:
                            active_ids[i] += filled_ids[i][-1:j-1:-1]
                            new_orders[i].clear()
                            filled_ids[i].clear()
                            break
                
                self._save_state()
                
        self.logger.info(f'Reached end time: {self.end_time}.')
        

    def run(self):
        """
        if not self._load_state():
            self._setup()
            
        self._run_with_retries()
        """
        super().run()
    

    def close(self):
        self._cancel_all_orders(self.category, self.symbol)

        self._close_position(self.category, self.symbol)

        # modify state file to remove active orders
        self.status = 'Closed'
        self._save_state()
        
        self.logger.info(f'Closed.')

        
class PairTrade(Strategy):
    def __init__(self, 
                 api_key, api_secret, demo, 
                 start_time, end_time, 
                 long_coin, short_coin, 
                 leverage, total_investment,
                 sl_pct, logger_level=logging.INFO):

        super().__init__(api_key, api_secret, demo)
        
        start_time = pd.Timestamp(start_time)
        end_time = pd.Timestamp(end_time)
        
        self._check_time(start_time, end_time)
        self._check_balance('USDT', total_investment)
        self._check_leverage('linear', f'{long_coin}USDT', leverage)
        self._check_leverage('linear', f'{short_coin}USDT', leverage)
        
        if sl_pct <= 0:
            raise ValueError('Stop loss percentage must be greater than 0')
        
        self.demo = demo
        self.start_time = start_time
        self.end_time = end_time
        self.long_coin = long_coin
        self.short_coin = short_coin
        self.leverage = leverage
        self.total_investment = total_investment
        self.sl_pct = sl_pct
        self.logger_level = logger_level if isinstance(logger_level, str) else logging._levelToName[logger_level]
        
        self.category = 'linear'
        self.long_symbol = f'{long_coin}USDT'
        self.short_symbol = f'{short_coin}USDT'
        
        self._set_logger(logger_level)

    
    @property
    def params(self):
        return {
            'api_key': 'YOUR_API_KEY',
            'api_secret': 'YOUR_API_SECRET',
            'demo': self.demo,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'category': self.category,
            'long_symbol': self.long_symbol,
            'short_symbol': self.short_symbol,
            'leverage': self.leverage,
            'total_investment': self.total_investment,
            'sl_pct': self.sl_pct,
            'logger_level': self.logger_level,
        }
        
        
    @property
    def state_params(self):
        pass
    
    
    @property
    def id(self):
        if hasattr(self, '_id'):
            return self._id
        
        self._id = f'{self.long_coin}{self.short_coin}_{self.start_time.strftime("%y%m%d%H%M%S")}'
        return self._id
    
    
    @property
    def max_loss(self):
        if hasattr(self, '_max_loss'):
            return self._max_loss
        
        self._max_loss = self.total_investment * self.sl_pct
        return self._max_loss
    
    
    def _get_qty(self, symbol):
        instruments_info = self.session.get_instruments_info(
            category=self.category,
            symbol=symbol,
        )['result']['list'][0]
        minOrderQty = float(instruments_info['lotSizeFilter']['minOrderQty'])
        qtyStep = float(instruments_info['lotSizeFilter']['qtyStep'])
        qty = self.total_investment * self.leverage / float(self.session.get_tickers(
            category=self.category,
            symbol=symbol,
        )['result']['list'][0]['markPrice'])
        decimal = int(-math.log10(qtyStep))
        qty = round((qty - minOrderQty) // qtyStep * qtyStep + minOrderQty, decimal)
        
        return qty
    
    
    @property
    def orders(self):
        if hasattr(self, '_orders'):
            return self._orders
        
        self.long_qty = self._get_qty(self.long_symbol)
        self.short_qty = self._get_qty(self.short_symbol)
        
        self._orders = {}
        
        self._orders['op'] = [
            {
                'symbol': self.long_symbol,
                'side': 'Buy',
                'orderType': 'Market',
                'qty': str(self.long_qty),
                'orderLinkId': f'{self.id}-long',
            },
            {
                'symbol': self.short_symbol,
                'side': 'Sell',
                'orderType': 'Market',
                'qty': str(self.short_qty),
                'orderLinkId': f'{self.id}-short',
            },
        ]
        
        self._orders['cl'] = [
            {
                'symbol': self.long_symbol,
                'side': 'Sell',
                'orderType': 'Market',
                'qty': str(self.long_qty),
                'orderLinkId': f'{self.id}-closeLong',
                'reduceOnly': True,
            },
            {
                'symbol': self.short_symbol,
                'side': 'Buy',
                'orderType': 'Market',
                'qty': str(self.short_qty),
                'orderLinkId': f'{self.id}-closeShort',
                'reduceOnly': True,
            },
        ]
        
        return self._orders
        
        
    def _setup(self):
        self.logger.info(f'\n{self.__repr__()}')

        self.session.cancel_all_orders(
            category=self.category,
            settleCoin='USDT',
        )
        positions = self.session.get_positions(
            category=self.category,
            settleCoin='USDT',
        )['result']['list']
        if positions:
            os.remove(self.log_file)
            raise AssertionError('Found open positions. Please close them before running the bot.')
        
        self._to_maxlev(self.category, self.long_symbol)
        self._to_maxlev(self.category, self.short_symbol)
        self._to_cross_margin()
        
        self._waiting(start_time=self.start_time)
        
        r = self.session.place_batch_order(
            category=self.category,
            request=self.op_orders,
        )
        if r['retCode'] != 0:
            raise AssertionError(f'Error placing orders: {r["retMsg"]}')
        for result, retExtInfo, order in zip(r['result']['list'], r['retExtInfo']['list'], self.op_orders):
            if retExtInfo['code'] != 0:
                self.logger.warning(f'Error placing {result["orderLinkId"]}: {retExtInfo["msg"]}')
            else:
                self.logger.info(f'Placed: {result["orderLinkId"]} ({order["side"]} at Market)')
        
        self._save_state()
        
        
    def _main_loop(self):
        while datetime.now() < self.end_time:
            positions = self.session.get_positions(
                category=self.category,
                settleCoin='USDT',
            )['result']['list']
            
            unrealisedPnl = sum(
                float(position['unrealisedPnl']) for position in positions
            )
            if unrealisedPnl < -self.max_loss:
                self.logger.info(f'Max loss reached: {unrealisedPnl}')
                break
            
        self.logger.info(f'Reached end time: {self.end_time}.')


    def close(self):
        self.session.cancel_all_orders(
            category=self.category,
            settleCoin='USDT',
        )
        r = self.session.place_batch_order(
            category=self.category,
            request=self.cl_orders,
        )
        if r['retCode'] != 0:
            raise AssertionError(f'Error placing orders: {r["retMsg"]}')
        for result, retExtInfo, order in zip(r['result']['list'], r['retExtInfo']['list'], self.cl_orders):
            if retExtInfo['code'] != 0:
                self.logger.warning(f'Error placing {result["orderLinkId"]}: {retExtInfo["msg"]}')
            else:
                self.logger.info(f'Placed: {result["orderLinkId"]} ({order["side"]} at Market)')
                
        self.status = 'Closed'
        self._save_state()

        self.logger.info(f'Closed.')
        

if __name__ == '__main__':
    account = 'demo'
    with open('./api.json', 'r') as f:
        data = json.load(f)[account]
        
    api_key = data['api_key']
    api_secret = data['api_secret']
    demo = data['demo']
    bot = GridBot(
        # start_time = '2025-06-21 03:17:50',
        start_time = datetime.now(),
        # end_time = '2025-05-30 00:00',
        end_time = datetime.now() + timedelta(hours=1),
        api_key=api_key,
        api_secret=api_secret,
        demo=demo,
        category='linear',
        symbol='BTCUSDT',
        low=106500,
        high=107500,
        mode='Arithmetic',
        ngrid=100,
        leverage=10,
        total_investment=10000,
        low_sl=106400,
        high_sl=107600,
        update_period=0,
        logger_level='DEBUG',
    )
    
    # bot = PairTrade(
    #     start_time = datetime.now(),
    #     end_time = '2025-05-24 11:00',
    #     api_key=api_key,
    #     api_secret=api_secret,
    #     demo=demo,
    #     long_coin='BTC',
    #     short_coin='BNB',
    #     leverage=10,
    #     total_investment=10000,
    #     sl_pct=0.01,
    #     logger_level=logging.DEBUG,
    # )
    
    bot.run()

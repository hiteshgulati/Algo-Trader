import kiteconnect
import pandas as pd
import numpy as np
import warnings
from pandas.core.frame import DataFrame
from pandas.core.indexes.base import Index
warnings.filterwarnings('ignore')
from py_vollib.black_scholes.implied_volatility import implied_volatility as bs_iv
from py_vollib.black_scholes.greeks import analytical as greeks
import datetime
import random
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


formatter = logging.Formatter('%(levelname)s|%(asctime)s|%(className)s->%(funcName)s|%(message)s')

timestamp_string = datetime.datetime.now().strftime("%Y-%m-%d %H_%M_%S")
file_handler = logging.FileHandler(f'./logs/{timestamp_string}kite_team.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

class Knowledge_guy:
    def __init__(self, kite_object) -> None:
        self.kite = kite_object
        nse = pd.DataFrame(self.kite.instruments("NSE"))
        nfo = pd.DataFrame(self.kite.instruments("NFO"))
        self.instruments = nfo.append(nse)
        self.instruments['exchange:tradingsymbol'] = self.instruments['exchange'] + ":" +self.instruments['tradingsymbol']
        self.next_nifty_expiry_date = self.instruments[(self.instruments['name']=='NIFTY')
                                    &(self.instruments['instrument_type']=='CE')]['expiry'].min()
        self.class_name_dict_for_logger = {'className':self.__class__.__name__}
        logger.info(f"""Kowledge Guy is Ready\tnext expiry date:{self.next_nifty_expiry_date.strftime("%d/%b/%Y")}""", extra=self.class_name_dict_for_logger)
        print(f"""Kowledge Guy is Ready\tnext expiry date:{self.next_nifty_expiry_date.strftime("%d/%b/%Y")}""")
    
    def instrumentLookup_by_symbol(self,symbol,field=['exchange:tradingsymbol']) -> float:
        try:
            ex_tradingsymbol = self.instruments[self.instruments['tradingsymbol']==symbol][field].iloc[0]
            if not isinstance(ex_tradingsymbol,str):
                ex_tradingsymbol = ex_tradingsymbol.iloc[0]
            return ex_tradingsymbol
        except:
            return -1
        
    def instrumentLookup_by_strike(self,strike,underlying='NIFTY', call_put='CE',expiry_date=None,field='exchange:tradingsymbol') ->float:
        if not expiry_date:
            if underlying == 'NIFTY':
                expiry_date = self.next_nifty_expiry_date
            else:
                expiry_date = self.instruments[(self.instruments['name']==underlying)
                                    &(self.instruments['instrument_type']==call_put)]['expiry'].min()
        strike = int(round(strike/50,0)*50)
        
        ex_tradingsymbol =  self.instruments[(self.instruments['name']==underlying)&(self.instruments['instrument_type']==call_put)
                        &(self.instruments['expiry']==expiry_date)&(self.instruments['strike']==strike)][field].iloc[0]
        if not isinstance(ex_tradingsymbol,str):
            ex_tradingsymbol = ex_tradingsymbol.iloc[0]
        logger.info(f"{strike}, {call_put}:\t{ex_tradingsymbol}", extra=self.class_name_dict_for_logger)
        return ex_tradingsymbol

    def available_strikes (self,underlying='NIFTY', call_put='CE',expiry_date=None) -> list:
        if underlying == 'NIFTY 50':
            underlying = 'NIFTY'
        if not expiry_date:
            expiry_date = self.instruments[(self.instruments['name']==underlying)
                                    &(self.instruments['instrument_type']==call_put)]['expiry'].min()
        return self.instruments[(self.instruments['name']==underlying)&(self.instruments['instrument_type']==call_put)
                        &(self.instruments['expiry']==expiry_date)]['strike']

class Data_guy:
    
    def __init__(self,index_symbol,kite_object,knowledge_guy) -> None:
        self.index_symbol = index_symbol.upper()
        if self.index_symbol == 'NIFTY':
            self.index_symbol = 'NIFTY 50'
        self.index_ltp = None
        self.kite = kite_object
        self.knowledge_guy = knowledge_guy
        self.class_name_dict_for_logger = {'className':self.__class__.__name__}
        logger.info("Data Guy is Ready", extra=self.class_name_dict_for_logger)
        print("Data Guy is Ready")

    def get_index_ltp (self) -> int:
        ticker = self.knowledge_guy.instrumentLookup_by_symbol(self.index_symbol)
        try:
            self.index_ltp = self.kite.ltp(ticker)[ticker]['last_price']
        except Exception as e:
            logger.info('Exception in Index LTP', extra=self.class_name_dict_for_logger)
            # print(e, format(e))
        return self.index_ltp

    def get_instrument_ltp (self, ex_trading_symbol) -> float:
        try:
            ltp = self.index_ltp = self.kite.ltp(ex_trading_symbol)[ex_trading_symbol]['last_price']
        except:
            ltp = 0
            logger.info(f'Exception in instrument LTP {ex_trading_symbol}', extra=self.class_name_dict_for_logger)
        return ltp
        
class Algo_analyst:
    
    def __init__(self,algo_name,index_symbol,knowledge_guy,max_rupee_loss,nifty_options_step = 50, tradelot = 50, entry_time = datetime.time(9,30,0),close_time = datetime.time(15,20,0)) -> None:
        self.orderbook = []
        self.current_orderbook = []
        self.algo_name = algo_name
        self.build = {'strategy':'away','low':0,'high':0}
        self.entry_time = entry_time
        self.exit_time = close_time
        self.nifty_options_step = nifty_options_step
        self.tradelot = tradelot
        self.active = False
        self.in_grasp = False
        self.knowledge_guy = knowledge_guy
        self.index_symbol = index_symbol.upper()
        if self.index_symbol == 'NIFTY 50':
            self.index_symbol = 'NIFTY'
        self.max_rupee_loss = max_rupee_loss
        self.class_name_dict_for_logger = {'className':self.__class__.__name__}
        logger.info("Analyst is Ready", extra=self.class_name_dict_for_logger)
        print("Analyst is Ready")

    def analyse_risk (self, pnl, trader_object, underlying_ltp) -> None:
        logger.info(f'NIFTY @ {underlying_ltp}', extra=self.class_name_dict_for_logger)
        if self.active:
            if pnl <= self.max_rupee_loss:
                logger.info(f'pnl={pnl}, closing all positions', extra=self.class_name_dict_for_logger)
                trader_object.close_all_positions()
                self.active = False
            else:
                logger.info(f'pnl={pnl}, safe to trade', extra=self.class_name_dict_for_logger)
            
            if self.in_grasp:
                if (self.build['strategy'] == 'strangle'):
                    if (underlying_ltp - self.build['high'] > (self.nifty_options_step * .1)) & \
                        (self.build['low'] - underlying_ltp > (self.nifty_options_step * .1)):
                        logger.warning("NIFTY went out of grasp, going away till NIFTY in grasp again", extra=self.class_name_dict_for_logger)
                        trader_object.close_all_positions()
                        self.in_grasp = False
                        self.build['strategy'] = 'away'

                elif (self.build['strategy'] == 'straddle'):
                    if abs(self.build['low'] - underlying_ltp) > (self.nifty_options_step * 1.1):
                        logger.warning("NIFTY went out of grasp, going away till NIFTY in grasp again", extra=self.class_name_dict_for_logger)
                        trader_object.close_all_positions()
                        self.in_grasp = False
                        self.build['strategy'] = 'away'
                pass
    
    def check_time (self,trader_object) -> None:
        if self.active:
            if datetime.datetime.now().time() > self.exit_time:
                logger.info(f'{datetime.datetime.now().time()} Times Up. Closing all positions', extra=self.class_name_dict_for_logger)
                trader_object.close_all_positions()
                self.active = False
                self.in_grasp = False
                self.build['strategy'] = 'away'
        elif datetime.datetime.now().time() >= self.entry_time:
            self.active=True

    def analyse_data (self,underlying_ltp):
        logger.info(f'NIFTY @ {underlying_ltp}, Build -> {str(self.build)}', extra=self.class_name_dict_for_logger)
        self.current_orderbook.clear()
        if self.active:
            if self.build['strategy'] == "away":
                available_strikes = self.knowledge_guy.available_strikes(underlying=self.index_symbol)
                closeness = available_strikes - underlying_ltp

                if closeness.abs().min() < (self.nifty_options_step * .1):
                    strike = available_strikes[closeness.abs().idxmin()]
                    logger.warning(f'Found entry position {strike}', extra=self.class_name_dict_for_logger)
                    

                    self.enter_orders(instrument={'strike':strike,'call_put':'CE'}, 
                                     transaction_type='sell',quantity = self.tradelot)
                    self.enter_orders(instrument={'strike':strike,'call_put':'PE'}, 
                                     transaction_type='sell',quantity = self.tradelot)
                    
                    self.build = {'strategy':'straddle','low':strike,'high':strike}
                    self.in_grasp = True
                    logger.warning(f'Build{self.build}', extra=self.class_name_dict_for_logger)
                    
            
            elif self.build['strategy'] == "straddle":
                if underlying_ltp - self.build['high'] > self.nifty_options_step/2:
                    
                    strike = self.build['high'] + self.nifty_options_step
                    logger.warning(f'Drift to High Strike: {strike}', extra=self.class_name_dict_for_logger)
                    

                    self.enter_orders(instrument={'strike':self.build['high'],'call_put':'PE'}, 
                                     transaction_type='buy',quantity = self.tradelot)
                    self.enter_orders(instrument={'strike':strike,'call_put':'PE'}, 
                                     transaction_type='sell',quantity = self.tradelot)

                    self.build['strategy'] = 'strangle'
                    self.build['high'] = strike
                    logger.warning(f'Build{self.build}', extra=self.class_name_dict_for_logger)
                    
                
                elif underlying_ltp - self.build['low'] < (self.nifty_options_step/2 * -1):

                    strike = self.build['low'] - self.nifty_options_step
                    logger.warning(f'Drift to Low Strike: {strike}', extra=self.class_name_dict_for_logger)
                    
                    self.enter_orders(instrument={'strike':self.build['low'],'call_put':'CE'}, 
                                     transaction_type='buy',quantity = self.tradelot)
                    self.enter_orders(instrument={'strike':strike,'call_put':'CE'}, 
                                     transaction_type='sell',quantity = self.tradelot)

                    self.build['strategy'] = 'strangle'
                    self.build['low'] = strike
                    logger.warning(f'Build{self.build}', extra=self.class_name_dict_for_logger)
                    

            elif self.build['strategy'] == "strangle":
                if underlying_ltp - self.build['high'] > (self.nifty_options_step * -.1):
                    strike = self.build['high']
                    logger.warning(f'Point capture at High Strike: {strike}', extra=self.class_name_dict_for_logger)
                    
                    self.enter_orders(instrument={'strike':self.build['low'],'call_put':'CE'}, 
                                     transaction_type='buy',quantity = self.tradelot)
                    self.enter_orders(instrument={'strike':strike,'call_put':'CE'}, 
                                     transaction_type='sell',quantity = self.tradelot)

                    self.build['strategy'] = 'straddle'
                    self.build['low'] = strike
                    logger.warning(f'Build{self.build}', extra=self.class_name_dict_for_logger)
                    

                elif underlying_ltp - self.build['low'] < (self.nifty_options_step * .1):
                    strike = self.build['low']
                    logger.warning(f'Point capture at Low Strike: {strike}', extra=self.class_name_dict_for_logger)
                    
                    self.enter_orders(instrument={'strike':self.build['high'],'call_put':'PE'}, 
                                     transaction_type='buy',quantity = self.tradelot)
                    self.enter_orders(instrument={'strike':strike,'call_put':'PE'}, 
                                     transaction_type='sell',quantity = self.tradelot)

                    self.build['strategy'] = 'straddle'
                    self.build['high'] = strike 
                    logger.warning(f'Build{self.build}', extra=self.class_name_dict_for_logger)
                    
        pass

    def enter_orders(self,instrument, transaction_type, quantity):
        ex_tradingsymbol = self.knowledge_guy.instrumentLookup_by_strike(strike=instrument['strike'],
            underlying=self.index_symbol, call_put=instrument['call_put'],expiry_date=None,field='exchange:tradingsymbol')
        order = {'ex_tradingsymbol':ex_tradingsymbol,'transaction_type':transaction_type,'quantity':quantity}
        self.orderbook.append(order)
        self.current_orderbook.append(order)   
        logger.warning(f'Order entered {order}', extra=self.class_name_dict_for_logger)
        
    def get_orderbook (self) -> list:
        return self.orderbook

    def get_current_orderbook (self) -> list:
        return self.current_orderbook

class Trader:

    def __init__ (self, kite_object, knowledge_guy) -> None:
        self.kite = kite_object
        self.knowledge_guy = knowledge_guy
        self.class_name_dict_for_logger = {'className':self.__class__.__name__}
        logger.info("Trader is Ready", extra=self.class_name_dict_for_logger)
        print("Trader is Ready")
        pass
    
    @staticmethod
    def placeMarketOrder(ex_tradingsymbol,transaction_type,quantity, kite) -> None:    
    # Place an intraday market order on NSE
        if transaction_type == "buy":
            t_type=kite.TRANSACTION_TYPE_BUY
        elif transaction_type == "sell":
            t_type=kite.TRANSACTION_TYPE_SELL
        kite.place_order(tradingsymbol=ex_tradingsymbol,
                        exchange=kite.EXCHANGE_NSE,
                        transaction_type=t_type,
                        quantity=quantity,
                        order_type=kite.ORDER_TYPE_MARKET,
                        product=kite.PRODUCT_MIS,
                        variety=kite.VARIETY_REGULAR)
        logger.warning(f"Kite Order for {ex_tradingsymbol} {t_type}")
        
    
    def start_trading(self, current_orderbook) -> None:
        
        for _ in range(len(current_orderbook)):
            trade = current_orderbook.pop(0)
            
            Trader.placeMarketOrder(ex_tradingsymbol=trade['ex_tradingsymbol'], transaction_type=trade['transaction_type'],
                quantity=trade['quantity'],kite=self.kite)
            logger.warning(str(trade['quantity']),' ',trade['ex_tradingsymbol']," ",trade['transaction_type'], extra=self.class_name_dict_for_logger)

    def close_all_positions(self) -> None:
        positions = self.kite.positions()['net']
        for each_position in positions:
            ex_tradingsymbol = each_position['exchange'] + ":" + each_position['tradingsymbol']
            quantity = each_position['quantity']
            transaction_type = None
            if quantity > 0:
                transaction_type = 'sell'
            elif quantity < 0:
                transaction_type = 'buy'
                quantity = abs(quantity)
            if transaction_type:
                Trader.placeMarketOrder(ex_tradingsymbol=ex_tradingsymbol, transaction_type=transaction_type,
                        quantity=quantity,kite=self.kite)
        pass

class PaperTrader (Trader):

    def __init__(self, kite_object, knowledge_guy, data_guy) -> None:
        self.data_guy = data_guy
        super().__init__(kite_object, knowledge_guy)

    def start_trading(self, current_orderbook) -> None:
        for _ in range(len(current_orderbook)):
            trade = current_orderbook.pop(0)
            ltp = self.data_guy.get_instrument_ltp(trade['ex_tradingsymbol'])
            logger.warning(str(trade['quantity']) + ' ' + trade['ex_tradingsymbol'] + " " + trade['transaction_type'] + " @ " + str(ltp), extra=self.class_name_dict_for_logger)

    def close_all_positions(self) -> None:
        positions = self.kite.positions()['net']
        for each_position in positions:
            ex_tradingsymbol = each_position['exchange'] + ":" + each_position['tradingsymbol']
            quantity = each_position['quantity']
            transaction_type = None
            if quantity > 0:
                transaction_type = 'sell'
            elif quantity < 0:
                transaction_type = 'buy'
                quantity = abs(quantity)
            if transaction_type:
                ltp = self.data_guy.get_instrument_ltp(ex_tradingsymbol)
                logger.warning(str(quantity),' ',ex_tradingsymbol," ",transaction_type," @ ",str(ltp), extra=self.class_name_dict_for_logger)
                # Trader.placeMarketOrder(ex_tradingsymbol=ex_tradingsymbol, transaction_type=transaction_type,
                        # quantity=quantity,kite=self.kite)

class Bookkeeper:
    
    def __init__(self, kite_object):
        self.kite = kite_object
        self.running_pnl = 0
        self.positions = pd.DataFrame()
        self.class_name_dict_for_logger = {'className':self.__class__.__name__}
        logger.info("Bookkeper is Ready", extra=self.class_name_dict_for_logger)
        print("Bookkeeper is Ready")
        pass

    def update (self):
        self.positions = self.kite.positions()['net']
        self.running_pnl = 0
        for each_position in self.positions:
            self.running_pnl += each_position['pnl']
        logger.info(f'Updated pnl\t{self.running_pnl}, positions:\t{self.positions}', extra=self.class_name_dict_for_logger)
    
    @property
    def pnl (self):
        self.update()
        return self.running_pnl
    @property
    def net_positions (self):
        self.update()
        return self.positions



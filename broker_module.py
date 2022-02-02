from xmlrpc.client import Boolean
import pandas as pd
from datetime import datetime, timedelta
import kiteconnect
from kiteconnect import KiteConnect
# import kite_connection
from ks_api_client import ks_api
from functools import wraps
from time import perf_counter_ns, perf_counter
import os

logger1 = None

def keep_log (**kwargs_decorator):
    """
    Decorator function used to log for functions.
    This decorator will log when:
        1) Function is called
        2) Function is executed, 
            along with result
        3) If function had error, 
            along with error message
        4) Time the function and report in milliseconds

    Args:
        level: level used to log for the function
        default_return: default return value in case of error
        kwargs: Additional parameters which will be logged
    """   
    default_return = None
    if 'default_return' in kwargs_decorator:
        default_return = kwargs_decorator.pop('default_return')
    def decorator_function (original_function):
        @wraps(original_function)
        def wrapper_function(*args,**kwargs):
            class_function_name_dict = {\
                    'className': args[0].__class__.__name__,
                    'functionName': original_function.__name__}
            try:
                logger1.log(status="Called",extra=class_function_name_dict,**kwargs_decorator,**{'args':args[1:]},**kwargs)
            except:
                pass
            start_time = perf_counter_ns()
            try:
                result = original_function(*args,**kwargs)
                end_time = perf_counter_ns()
                logger1.log(result=result,
                    extra=class_function_name_dict,
                    status="End",execution_time = (end_time-start_time)/1_000_000,
                    **kwargs_decorator)
                return result
            except Exception as e:
                logger1.log(status="Exception",e=e,extra=class_function_name_dict,**kwargs_decorator)
                return default_return
        return wrapper_function
    return decorator_function


class Broker:
    """
    Broker serves as a bridge between trader and market broker (like Zerodha)
    Broker initiates objects for various brokers available in market. 
    It takes standardized inputs from algo_module and converts them 
    based on the applicable APIs af market broker.
    There can be two types of brokers:
        1) Broker for Data - This broker will be used to fetch data,
            there will be multiple calls per iteration to this broker,
            so we need to make sure this broker supports multiple API calls,
            at least 150 per second (iteration)
        2) Broker for Trade - This broker will be used to place trades.
            We will use this broker to place trade, and thus we need to ensure 
            that there is sufficient capital for trades. There can be multiple trades 
            and thus a low cost per trade broker will be preferred.

            (T): Represents functionality is applicable only to Trade Broker
            (D): Represents functionality is applicable only to Data Broker
            (T/D): Represents functionality is applicable to both Trade and Data Broker
    
    Available Brokers:
        - Zerodha aka kite (T/D)
        - Kotak (T/D)
        - Paper used while doing Paper trading (T)

    Available functionality:
        - set_broker_object: (T/D) Initialize broker object 
            using access tokens, and prepare instruments book 
            used to fetch instrument id 
        - get_fno_instrument_id: (T/D) Get instrument ID of 
            an FnO product
        - get_multiple_fno_instrument_id: (T/D) Get 
            instrument ID of multiple FnO products
        - place_market_order: (T) Place intraday market order 
            on the broker
        - cancel_order: (T) Cancel previously place order 
            using broker_order_id
        - get_ltp: (D) Get LTP of desired instrument
        - get_multiple_ltp: (D) Get LTP of multiple 
            instruments at once
        - get_positions: (T) Get day's positions from 
            market broker
        - get_pnl: (T) Get current pnl from market broker,
            this doesn't include broker's fee
        - get_next_expiry_datetime: (D) Get datetime for 
            next expiry
        - get_available_strikes: (D) Get available strikes 
            currently trading in the market
        - is_order_complete: (T) Get confirmation if 
            the order is complete using broker_order_id

    """    


    def __init__(self,logger=None) -> None:
        """Initialize blank broker object

        Args:
            logger (Logger, optional): Logger object. 
                Defaults to None.
        """        
        global logger1
        logger1 = logger
        pass

    @keep_log()
    def set_parameters(self, broker_for_trade, broker_for_data,
                historical_data_folder_name, underlying_name,
                fno_folder_name,equity_folder_name,
                data_guy=None,
                kite_api_key=None, kite_access_token=None,
                kotak_consumer_key=None, kotak_access_token=None,
                kotak_consumer_secret=None,kotak_user_id=None,
                kotak_access_code=None, kotak_user_password=None,
                logger=None, current_datetime=None) -> None:
        """Set Parameters for broker object

        Variables used by Broker object:
            logger1: Used to maintain logs
            data_guy: Used to fetch current datetime 
            broker_for_trade: Name of Broker used for trading
            broker_for_data: Name of Broker used for data
            kite: Zerodha/Kite object
            kotak: Kotak object
            tradebook: df used to maintain day's trade 
                while using paper broker
            paper_trade_realized_pnl: used to maintain 
                day's pnl while using paper broker
            positions_book: df used to maintain current positions


        Args:
            broker_for_trade (str): Name of broker used for trading
            broker_for_data (str): Name of broker used to fetch data
            data_guy (Data_guy, optional): data_guy object. 
                Used to fetch the current datetime of the simulation. 
                Defaults to None.
            kite_api_key (str, optional): API key for Kite object, 
                used only if Zerodha/Kite is being used as a broker. 
                Defaults to None.
            kite_access_token (str, optional): Kite Access Token, 
                used only if Zerodha/Kite is being used as a broker. 
                Defaults to None.
            kotak_consumer_key (str, optional): Consumer Key for Kotak, 
                used only if Kotak is being used as a broker. 
                Defaults to None.
            kotak_access_token (str, optional): Access Token for Kotak, 
                used only if Kotak is being used as a broker. 
                Defaults to None.
            kotak_consumer_secret (str, optional): Consumer Secret Key for Kotak, 
                used only if Kotak is being used as a broker. 
                Defaults to None.
            kotak_user_id (str, optional): User ID for Kotak, 
                used only if Kotak is being used as a broker. 
                Defaults to None.
            kotak_access_code (str, optional): Access Code for Kotak, 
                used only if Kotak is being used as a broker. 
                Defaults to None.
            kotak_user_password (str, optional): Password of Kotak, 
                used only if Kotak is being used as a broker. 
                Defaults to None.
            logger (Logger, optional): Logger object used to maintain logs. Defaults to None.
            current_datetime (datetime.datetime, optional): Current Datetime of simulation. Defaults to None.
        """            
        if current_datetime is None: current_datetime = datetime.now()
        global logger1
        #Assign logger if not done while initializing object
        if logger1 is None: logger1 = logger
        
        #Data guy object used to fetch current datetime 
        #   of the simulation
        self.data_guy = data_guy
        self.broker_for_trade = broker_for_trade.upper()
        self.broker_for_data = broker_for_data.upper()
        self.kite = None
        self.kotak = None
        self.sim = None

        #Instruments Book contains all 
        #   available instruments in the market 
        #   and its correspoing details like: 
        #   instrument id, expiry datetime
        self.kite_instruments_book = None
        self.kotak_instruments_book = None

        #Set market broker object for Trade Broker
        self.set_broker_object(broker_name=self.broker_for_trade,
                            current_datetime=current_datetime,
                            kite_api_key = kite_api_key,
                            kite_access_token=kite_access_token,
                            kotak_access_code=kotak_access_code,
                            kotak_consumer_secret=kotak_consumer_secret,
                            kotak_access_token=kotak_access_token,
                            kotak_user_id=kotak_user_id,
                            kotak_user_password=kotak_user_password,
                            kotak_consumer_key=kotak_consumer_key,
                            underlying_name=underlying_name,
                            historical_data_folder_name = historical_data_folder_name,
                            fno_folder_name = fno_folder_name,
                            equity_folder_name = equity_folder_name)

        #Set market broker object for Data Broker 
        #   if not same as Trade Broker
        if self.broker_for_data != self.broker_for_trade:
            self.set_broker_object(broker_name=self.broker_for_data,
                            current_datetime=current_datetime,
                            kite_api_key = kite_api_key,
                            kite_access_token=kite_access_token,
                            kotak_access_code=kotak_access_code,
                            kotak_consumer_secret=kotak_consumer_secret,
                            kotak_access_token=kotak_access_token,
                            kotak_user_id=kotak_user_id,
                            kotak_user_password=kotak_user_password,
                            kotak_consumer_key=kotak_consumer_key,
                            underlying_name=underlying_name,
                            historical_data_folder_name = historical_data_folder_name,
                            fno_folder_name = fno_folder_name,
                            equity_folder_name = equity_folder_name)
        
        #df to maintain current positions
        self.positions_book = pd.DataFrame()

        #df to maintain tradebook containing 
        #   all trades done during the day 
        #   for Paper Trade
        self.tradebook = pd.DataFrame()
        #Name and path to save tradebook
        self.trades_df_name = f'''trades_df/Trades\
                    {current_datetime.strftime("%Y-%m-%d-%H-%M-%S")}.csv'''\
                    .replace(" ","")
        #pnl if using paper trade
        self.paper_trade_realized_pnl = 0



    @keep_log(default_return=False)
    def set_broker_object(self,broker_name,underlying_name,
                            historical_data_folder_name = None,
                            fno_folder_name = None,
                            equity_folder_name = None,
                            current_datetime = None,
                            kite_api_key=None, kite_access_token=None,
                            kotak_consumer_key=None, kotak_access_token=None,
                            kotak_consumer_secret=None,kotak_user_id=None,
                            kotak_access_code=None, kotak_user_password=None) -> bool:
        """ (T/D)
        Initialize market broker name and instrument df

        Args:
            broker_name (str): name of broker to be initialized eg Zerodha
            current_datetime (datetime.datetime, optional): Simulation datetime 
                to get date's instrument df. Defaults to None.
            kite_api_key (str, optional): API key for Kite object, 
                used only if Zerodha/Kite is being used as a broker. 
                Defaults to None.
            kite_access_token (str, optional): Kite Access Token, 
                used only if Zerodha/Kite is being used as a broker. 
                Defaults to None.
            kotak_consumer_key (str, optional): Consumer Key for Kotak, 
                used only if Kotak is being used as a broker. 
                Defaults to None.
            kotak_access_token (str, optional): Access Token for Kotak, 
                used only if Kotak is being used as a broker. 
                Defaults to None.
            kotak_consumer_secret (str, optional): Consumer Secret Key for Kotak, 
                used only if Kotak is being used as a broker. 
                Defaults to None.
            kotak_user_id (str, optional): User ID for Kotak, 
                used only if Kotak is being used as a broker. 
                Defaults to None.
            kotak_access_code (str, optional): Access Code for Kotak, 
                used only if Kotak is being used as a broker. 
                Defaults to None.
            kotak_user_password (str, optional): Password of Kotak, 
                used only if Kotak is being used as a broker. 
                Defaults to None.

        Returns:
            bool: True is the object is set successfully 
        """                           
        if broker_name == 'ZERODHA':
            self.kite = KiteConnect(api_key=kite_api_key)
            self.kite.set_access_token(kite_access_token)

            nse = pd.DataFrame(self.kite.instruments("NSE"))
            nfo = pd.DataFrame(self.kite.instruments("NFO"))
            self.kite_instruments_book = nfo.append(nse)
            self.kite_instruments_book['expiry'] =  pd.to_datetime(self.kite_instruments_book['expiry'], 
                                                    format='%Y-%m-%d') 
            self.kite_instruments_book['expiry_datetime'] = self.kite_instruments_book['expiry'] + \
                                                timedelta (hours=15, minutes =30)

            self.kite_instruments_book = self.kite_instruments_book[['tradingsymbol',\
                                                'name','strike','instrument_type',\
                                                'exchange','expiry','expiry_datetime']]
            return True
        
        elif broker_name == 'KOTAK':
            
            
            self.kotak = ks_api.KSTradeApi(access_token = kotak_access_token, userid = kotak_user_id, \
                                            consumer_key = kotak_consumer_key, ip = "127.0.0.1", 
                                            app_id = "app_id")

            self.kotak.login(password=kotak_user_password)
            
            a=self.kotak.session_2fa(access_code = kotak_access_code)

            if current_datetime is None:
                current_datetime = datetime.now()
            date_url = current_datetime.strftime("%d_%m_%Y")
            options_url_pre = "https://preferred.kotaksecurities.com/security/production/TradeApiInstruments_FNO_"
            options_url_post = ".txt"
            options_url = options_url_pre + date_url + options_url_post
            odf = pd.read_csv(options_url, sep='|')
            odf = odf[(odf['optionType'] != 'XX') & (odf['optionType'].notnull() )] \
                [['instrumentToken','instrumentName','strike','expiry','optionType']].drop_duplicates()
            odf['expiry_datetime'] = pd.to_datetime(odf['expiry'], format='%d%b%y') + timedelta (hours=15, minutes =30)
            
            equity_url_pre = "https://preferred.kotaksecurities.com/security/production/TradeApiInstruments_Cash_"
            equity_url_post = ".txt"
            equity_url = equity_url_pre + date_url + equity_url_post
            edf = pd.read_csv(equity_url, sep='|')
            edf = edf[['instrumentToken','instrumentName']].drop_duplicates()
            self.kotak_instruments_book = odf.append(edf, ignore_index=True)

            return True

        elif broker_name == "SIM":
            self.sim = Exchange()
            
            self.sim.set_parameters(current_datetime=current_datetime,
                underlying_name = underlying_name,
                historical_data_folder_name=historical_data_folder_name,
                fno_folder_name=fno_folder_name,
                equity_folder_name=equity_folder_name)
            return True

        elif broker_name == 'PAPER':
            return True
        elif broker_name == 'BACKTEST':
            ## INITIALIZE BACKTESTING OBJECT HERE
            return True


    @keep_log()
    def get_fno_instrument_id (self, broker_for, 
        strike, underlying, call_put, 
        expiry_datetime) -> str:
        """ (T/D)
            Get Instrument ID of desired FnO instrument 
            based on the broker

            Zerodha:
                Returns tradingsymbol of the instrument.
            Kotak:
                Returns instrumentToken of the instrument.
            Paper:
                Returns value based on the data broker.
                For eg ig data broker is Zerodha, 
                then tradingsymbol will be returned

        Args:
            broker_for (str): trade or data. 
                Instrument ID will be fetched 
                for trade broker or data broker 
            strike (int): Strike of the FnO instrument
            underlying (str): name of underlying for 
                FnO instrument eg NIFTY/BANKNIFTY
            call_put (str): Call or Put CE/PE
            expiry_datetime (datetime.datetime): 
                Expiry datetime of the desired FnO instrument

        Returns:
            str: Instrument ID of the desired instrument
        """        
        
        #instrument_id_broker represents the broker 
        #   for which instrument id will be returned
        # If trade broker is PAPER set this to data broker
        instrument_id_broker = self.broker_for_trade
        if (broker_for.upper() == 'DATA') | (self.broker_for_trade == 'PAPER'):
            instrument_id_broker = self.broker_for_data

        if instrument_id_broker == 'ZERODHA':  
            
            instrument_id = str(self.kite_instruments_book[(self.kite_instruments_book['name']==underlying)
                    &(self.kite_instruments_book['instrument_type']==call_put)
                    &(self.kite_instruments_book['expiry_datetime']==expiry_datetime)
                    &(self.kite_instruments_book['strike']==strike)]['tradingsymbol'].iloc[0])
            
            if not isinstance(instrument_id,str):
                instrument_id = str(instrument_id.iloc[0])
            

            return instrument_id

        elif instrument_id_broker == 'KOTAK':
            
            instrument_id = str(self.kotak_instruments_book[ (self.kotak_instruments_book['strike']==strike)
                & (self.kotak_instruments_book['instrumentName']==underlying) 
                & (self.kotak_instruments_book['expiry_datetime']==expiry_datetime)
                & (self.kotak_instruments_book['optionType']==call_put)]['instrumentToken'].iloc[0])
            

            return instrument_id

        elif instrument_id_broker == 'SIM':
            instrument_id = str(self.sim.instruments_book[(self.sim.instruments_book['underlying']==underlying)
                    &(self.sim.instruments_book['call_put']==call_put)
                    &(self.sim.instruments_book['expiry_datetime']==expiry_datetime)
                    &(self.sim.instruments_book['strike']==strike)]['ticker'].iloc[0])
        
            return instrument_id
        
        elif instrument_id_broker== 'BACKTEST':
            instrument_id = None 
            ## GET INSTRUMENT ID FOR BACKTEST
            return instrument_id


    @keep_log()
    def get_multiple_fno_instrument_id (self, broker_for, fno_df) -> pd.Series:
        """(T/D)
            Get Instrument ID of multiple FnO instruments
            based on the broker

            Zerodha:
                Returns tradingsymbol of the instrument.
            Kotak:
                Returns instrumentToken of the instrument.
            Paper:
                Returns value based on the data broker.
                For eg ig data broker is Zerodha, 
                then tradingsymbol will be returned

        Args:
            broker_for (str): trade or data. 
                Instrument ID will be fetched 
                for trade broker or data broker 
            fno_df (pd.DataFrame): df consisting 
                multiple fno instruments.
                FnO_df should have:
                    - underlying
                    - call_put
                    - expiry_datetime
                    - strike

        Returns:
            pd.Series: Instrument ID of the desired instrument
        """        
        
        #instrument_id_broker represents the broker 
        #   for which instrument id will be returned
        # If trade broker is PAPER set this to data broker
        
        # fno_df should have underlying, call_put, expiry_datetime and strike column
        
        instrument_id_broker = self.broker_for_trade
        if (broker_for.upper() == 'DATA') | (self.broker_for_trade == 'PAPER'): 
            instrument_id_broker = self.broker_for_data

        if instrument_id_broker == 'ZERODHA':  
            instrument_id = fno_df.merge(self.kite_instruments_book,how='left',\
                    left_on=['underlying','call_put','expiry_datetime','strike'], 
                    right_on=['name','instrument_type','expiry_datetime','strike'])\
                    ['tradingsymbol'].astype(str)
            

            return instrument_id

        elif instrument_id_broker == 'KOTAK':
            instrument_id = None 
            ## GET INSTRUMENT ID FOR KOTAK -> COMPLETE
            instrument_id = fno_df.merge(self.kotak_instruments_book,how='left',\
                    left_on=['underlying','call_put','expiry_datetime','strike'], 
                    right_on=['instrumentName','optionType','expiry_datetime','strike'])\
                    ['instrumentToken'].astype(str)

            return instrument_id

        elif instrument_id_broker == 'SIM':
            instrument_id = fno_df.merge(self.sim.instruments_book,how='left',\
                    left_on=['underlying','call_put','expiry_datetime','strike'], 
                    right_on=['underlying','call_put','expiry_datetime','strike'])\
                    ['ticker'].astype(str)


            return instrument_id

        elif instrument_id_broker == 'BACKTEST':
            instrument_id = None 
            ## GET INSTRUMENT ID FOR BACKTEST
            return instrument_id

    @keep_log()
    def place_market_order(self, instrument_id, 
        buy_sell, quantity, current_datetime, 
        initiation_time, exchange='NFO') -> str:
        """(T/D)
        Place Intraday market order on trade Broker

            Note for Paper trade
                - data broker is used
                - no order is punched 
                instead a df is maintained with 
                average price as per appropriate 
                buy/sell quotations 

        Args:
            instrument_id (str): instrument id as per trade broker
            buy_sell (str): 'buy' / 'sell'
            quantity (int): quantity of order, note this is quantity and not lot, 
                if lot size is 50 and 3 lots so quantity will be 150
            exchange (str, optional): Exchange eg NFO/NSE. Defaults to 'NFO'.

        Returns:
            str: broker_order_id
        """                      

        if self.broker_for_trade == "ZERODHA":   
            # Place an intraday market order on NSE
            if buy_sell == "buy":
                t_type=self.kite.TRANSACTION_TYPE_BUY
            elif buy_sell == "sell":
                t_type=self.kite.TRANSACTION_TYPE_SELL
            
            kite_exchange = None
            if exchange == 'NSE':
                kite_exchange = self.kite.EXCHANGE_NSE
            elif exchange == 'NFO':
                kite_exchange = self.kite.EXCHANGE_NFO
            
            broker_order_id = self.kite.place_order(tradingsymbol=instrument_id,
                            exchange=kite_exchange,
                            transaction_type=t_type,
                            quantity=quantity,
                            order_type=self.kite.ORDER_TYPE_MARKET,
                            product=self.kite.PRODUCT_MIS,
                            variety=self.kite.VARIETY_REGULAR)

            return broker_order_id

        elif self.broker_for_trade == "KOTAK":
            ## PLACE ORDER ON KOTAK
            instrument_id = int(instrument_id)
            broker_order = self.kotak.place_order(\
                            order_type='MIS',
                            instrument_token=instrument_id, 
                            transaction_type=buy_sell.upper(),
                            quantity=quantity,validity='GFD',
                            variety="REGULAR", price=0)

            broker_order_id = ""

            if list(broker_order)[0] == 'Success':
                broker_order_id = str(broker_order['Success'][list(broker_order['Success'])[0]]['orderId'])
                
            return broker_order_id

        # Note for Paper trade no order is punched 
        #     instead a df is maintained with 
        #     average price as per appropriate 
        #     buy/sell quotations 
        elif self.broker_for_trade == 'PAPER':
            
            buy_sell_counter_trade = 'buy'
            multiplier = -1
            if buy_sell == 'buy': 
                buy_sell_counter_trade = 'sell'
                multiplier = 1

            if self.broker_for_data == 'ZERODHA':
                order_instrument = ltp =  f'{exchange}:{instrument_id}'
                price = self.kite.quote(order_instrument)\
                    [order_instrument]\
                    ['depth']\
                    [buy_sell_counter_trade][0]\
                    ['price']
            
            elif self.broker_for_data == 'KOTAK':
                price = -1
            elif self.broker_for_data == "SIM":
                # order_instrument = ltp =  f'{exchange}:{instrument_id}'
                quote = self.sim.quote(instrument_id=instrument_id,
                    current_datetime=current_datetime, 
                    initiation_time=initiation_time)
                buy_sell_counter_trade = buy_sell_counter_trade + "_price"
                price = quote[buy_sell_counter_trade]
            else:
                price = -1
           
            broker_order_id = str(datetime.now())
            position = {'broker_order_id':broker_order_id,\
                        'instrument_id':instrument_id,\
                        'quantity':quantity*multiplier,\
                        'exchange':exchange,\
                        'average_price':price,\
                        'current_datetime':self.data_guy.current_datetime,\
                        'current_ltp':self.data_guy.current_ltp}
            logger1.log(xyzzyspoon0 = len(self.positions_book), positions_book=self.positions_book)
            logger1.log(price=price,position=position)
            self.positions_book = self.positions_book.append\
                                (position,ignore_index=True)
            logger1.log(xyzzyspoon1 = len(self.positions_book), positions_book=self.positions_book)
            self.tradebook = self.tradebook.append(\
                position,ignore_index=True)

            self.get_pnl(current_datetime=current_datetime,
                        initiation_time=initiation_time)
            self.positions_book.reset_index(inplace=True)
            logger1.log(xyzzyspoon2 = len(self.positions_book), positions_book=self.positions_book)
            self.tradebook.to_csv(self.trades_df_name
                    ,index=False)

            return broker_order_id

        elif self.broker_for_trade == "BACKTEST":
            ## PLACE ORDER ON BACKTEST
            return None


    @keep_log()
    def cancel_order(self, broker_order_id) -> Boolean:
        """(T)
        Cancel existing unexecuted order using broker_order_id

        Args:
            broker_order_id (str): id of order to be cancelled

        Returns:
            Boolean: True is order cancellation is sent
        """        

        if self.broker_for_trade == "ZERODHA":   
            # Place an intraday market order on NSE

            broker_order = self.kite.cancel_order(
                                variety=self.kite.VARIETY_REGULAR,\
                                order_id=broker_order_id)
                            
            order_cancelled = False

            return order_cancelled

        elif self.broker_for_trade == "KOTAK":
            ## PLACE ORDER ON KOTAK
            cancelled_order = self.kotak.cancel_order(broker_order_id)

            order_cancelled = False
            if list(cancelled_order)[0]=='Success':
                order_cancelled = True

            return order_cancelled

        #Not to be used for Paper trade as no order is punched
        elif self.broker_for_trade == "PAPER":
            return False

        elif self.broker_for_trade == "BACKTEST":
            ## PLACE ORDER ON BACKTEST
            return None


    @keep_log()
    def get_ltp (self, instrument_id, 
        current_datetime, initiation_time,
        exchange="NSE") -> float:
        """(D)
        Get current LTP. Note not applicable for Paper Broker

        Args:
            instrument_id (str): instrument for which LTP is fetched
            exchange (str, optional): NSE/NFO. Defaults to "NSE".

        Returns:
            float: LTP of instrument
        """        

        if self.broker_for_data == "ZERODHA":
            if instrument_id == 'NIFTY': instrument_id = "NIFTY 50"
            ltp =  self.kite.ltp(f'{exchange}:{instrument_id}') \
                [f'{exchange}:{instrument_id}'] \
                ['last_price']
            return ltp

        elif self.broker_for_data == "KOTAK":
            ## GET LTP FROM KOTAK
            if instrument_id == "NIFTY":
                instrument_id = str(self.kotak_instruments_book[\
                                self.kotak_instruments_book['instrumentName']=='NIFTY 50']\
                                    ['instrumentToken'].iloc[0])
            ltp =  float(self.kotak.quote(\
                instrument_token=instrument_id,quote_type='LTP')\
                ['success'][0]['lastPrice'])

            return ltp
        elif self.broker_for_data == 'SIM':
            if instrument_id == "NIFTY": instrument_id = "NIFTY 50.NSE_IDX"
            ltp =  self.sim.ltp(instrument_id,
                current_datetime=current_datetime, 
                initiation_time=initiation_time)
            return ltp


    @keep_log()
    def get_multiple_ltp (self, instruments_df, 
        current_datetime, initiation_time,
        exchange="NFO") -> pd.Series:
        """(D)
        Get current LTP of multiple instruments. 
        Note not applicable for Paper Broker

        Args:
            instruments_df (pd.DataFrame): df of multiple instruments 
                for which LTP is to be fetched. df should contain
                    - instrument_id column
            exchange (str, optional): NSE/NFO. Defaults to "NFO".

        Returns:
            pd.Series: Series of LTP of instrument
        """     
        ## instruments_df should have instrument_id_data column

        if self.broker_for_data == "ZERODHA":
            df = instruments_df.copy()
            df['exchange:instrument_id'] = exchange + ":" + df['instrument_id_data']
            last_price = pd.DataFrame.from_dict(self.kite.ltp(\
                            list(df['exchange:instrument_id']))\
                            ,orient='index')
            last_price['exchange:instrument_id'] = last_price.index
            ltp = df.merge(last_price,how='left',on='exchange:instrument_id')['last_price']

            return ltp

        elif self.broker_for_data == "KOTAK":
            ## GET LTP FROM KOTAK
            df = instruments_df.copy()
            df['ltp'] = None
            for idx,each_instrument in df.iterrows():
                instrument_id = each_instrument['instrument_id_data']
                df.loc[idx,'ltp'] = self.get_ltp(instrument_id=instrument_id,
                        current_datetime=current_datetime,
                        initiation_time=initiation_time)
            
            ltp = df['ltp']

            return ltp

        elif self.broker_for_data == 'SIM':
            df = instruments_df.copy()
            ltp = self.sim.ltp(instruments=df[['instrument_id_data']],
                current_datetime=current_datetime,
                initiation_time=initiation_time)['ltp']
            return ltp


    @keep_log()
    def get_positions (self) -> pd.DataFrame:
        """(T)
        Get current positions

        Returns:
            pd.DataFrame: df containing current positions
                positions_df contains:
                    - instrument_id_trade
                    - quantity
                    - exchange 
        """        

        if self.broker_for_trade == 'ZERODHA':
            positions = pd.DataFrame(self.kite.positions()['net'])
            positions.rename(columns={'tradingsymbol':'instrument_id_trade'}, inplace=True)

            return positions

        elif self.broker_for_trade == 'KOTAK':
            positions = self.kotak.positions(position_type='TODAYS')
            positions_df = pd.DataFrame()
            if list(positions)[0] == 'Success':
                positions_df = pd.DataFrame(positions['Success'])
                if len(positions_df) != 0:
                    positions_df['instrument_id_trade'] = positions_df['instrumentToken'].astype(str)
                    positions_df['quantity'] = positions_df['netTrdQtyLot']
                    positions_df['exchange'] = ""
                    positions_df = positions_df[['instrument_id_trade','quantity','exchange']]

            return positions_df

        elif self.broker_for_trade == 'PAPER':
            positions_df = self.positions_book.copy()
            positions_df['instrument_id_trade'] = positions_df['instrument_id']
            positions_df = positions_df[['instrument_id_trade','quantity','exchange']]

            return positions_df

    
    @keep_log()
    def get_pnl (self,current_datetime, initiation_time) -> float:
        """(T)
        Get day's pnl
        Steps followed:
            Zerodha:
              - Get current positions
              - take sum of pnl column
            Kotak:
              - Get current positions
              - Sell Value - Buy Value - unrealized pnl
            Paper:
              - Get LTP of all instruments in positions
              - calculate change in price 
                    by LTP - average price
              - calculate pnl using price change
              - remove squared off instruments from df
              - calculate average purchase price


        Returns:
            float: PnL
        """        

        if self.broker_for_trade == 'ZERODHA':
            positions = pd.DataFrame(self.kite.positions()['net'])
            if len(positions)==0:
                pnl = 0
            else:
                pnl =  round(positions['pnl'].sum(),2)
            return pnl

        elif self.broker_for_trade == 'KOTAK':
            
            positions = self.kotak.positions(position_type='TODAYS')
            pnl=0
            if list(positions)[0] == 'Success':
                positions_df = pd.DataFrame(positions['Success'])
                if len(positions_df) != 0:
                    positions_df['unrealized_pnl'] = positions_df['netTrdQtyLot']\
                                                        * positions_df['lastPrice']
                    pnl = positions_df['sellTradedVal'].sum()\
                            - positions_df['buyTradedVal'].sum()\
                            + positions_df['unrealized_pnl'].sum()

            return pnl

        elif self.broker_for_trade == 'PAPER':
            logger1.log(thwack=len(self.positions_book), positions_book=self.positions_book)
            if len(self.positions_book) == 0:
                return self.paper_trade_realized_pnl
            self.positions_book['instrument_id_data'] = \
                                    self.positions_book['instrument_id']
            # Get LTP of all instruments
            self.positions_book['ltp'] = self.get_multiple_ltp\
                                        (instruments_df = self.positions_book,\
                                        current_datetime = current_datetime, 
                                        initiation_time = initiation_time,
                                        exchange='NFO')
            #Calculate change in price by LTP - average price
            self.positions_book['price_change'] = self.positions_book['ltp']\
                                        - self.positions_book['average_price']
            self.positions_book['pnl'] = (self.positions_book['price_change']\
                                        * self.positions_book['quantity'])\
                                            .round(decimals=2)

            pnl = round(self.paper_trade_realized_pnl \
                + self.positions_book['pnl'].sum(),2)
            logger1.log(thwack2=len(self.positions_book), positions_book=self.positions_book)
            #Aggregate df to remove squared off positions
            self.positions_book = self.positions_book[['instrument_id',\
                                'exchange','quantity','ltp',\
                                'pnl']]\
                                .groupby(['instrument_id',\
                                    'exchange','ltp'])\
                                .sum().reset_index()
            logger1.log(thwack3=len(self.positions_book), positions_book=self.positions_book)
            self.paper_trade_realized_pnl += round(self.positions_book[\
                            self.positions_book['quantity']==0]\
                            ['pnl'].sum(),2)
            logger1.log(thwack4=len(self.positions_book), positions_book=self.positions_book)
            self.positions_book = self.positions_book[\
                            self.positions_book['quantity']!=0]
            logger1.log(thwack5=len(self.positions_book), positions_book=self.positions_book)
            self.positions_book['average_price'] = self.positions_book['ltp'] - \
                        (\
                            self.positions_book['pnl']\
                            / self.positions_book['quantity']
                            )

            #Save current positions book by datetime string
            # self.positions_book.to_csv(f'''interim_df/positions\
            #         {datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}.csv'''\
            #         .replace(" ","")
            #         , index=False)
            logger1.log(thwack6=len(self.positions_book), positions_book=self.positions_book)
            return pnl
            
        elif self.broker_for_trade == 'BACKTEST':
            ## GET PNL FROM BACKTEST
            return None


    @keep_log()
    def get_next_expiry_datetime(self, underlying='NIFTY') -> datetime:
        """(D) - data broker is used, 
            execution is managed internally 
            by broker class using instruments_Df
        Get datetime of next expiry for 
            specified underlying using instruments df
            Not applicable for Paper Broker

        Args:
            underlying (str, optional): name of underlying. 
                Defaults to 'NIFTY'.

        Returns:
            datetime: expiry datetime of next expiring FnO
        """        

        if self.broker_for_data == 'ZERODHA':
            next_expiry_datetime = self.kite_instruments_book[\
                                    (self.kite_instruments_book['name']==underlying.upper()) 
                                    & (self.kite_instruments_book['instrument_type'] == 'CE')]\
                                    ['expiry_datetime'].min()

            return next_expiry_datetime

        elif self.broker_for_data == 'KOTAK':
            next_expiry_datetime = self.kotak_instruments_book[(\
                                self.kotak_instruments_book['instrumentName']==underlying.upper())\
                            & (self.kotak_instruments_book['optionType']=='CE')]\
                            ['expiry_datetime'].min()
            
            return next_expiry_datetime

        elif self.broker_for_data == 'SIM':
            next_expiry_datetime = self.sim.instruments_book[\
                                    (self.sim.instruments_book['underlying']==underlying.upper()) 
                                    & (self.sim.instruments_book['call_put'] == 'CE')]\
                                    ['expiry_datetime'].min()
            return next_expiry_datetime


    @keep_log()
    def get_available_strikes (self, underlying, call_put, expiry_datetime) -> list:
        """(D) - data broker is used, 
            execution is managed internally 
            by broker class using instruments_Df
        Get strikes of currently traded Options

        Args:
            underlying (str): name of underlying
            call_put (str): CE/PE
            expiry_datetime (datetime.datetime): expiry datetime

        Returns:
            list: list of all strikes of currently traded options
        """        


        if self.broker_for_data == 'ZERODHA':
            available_strikes = self.kite_instruments_book[(self.kite_instruments_book['name']==underlying) 
                                            & (self.kite_instruments_book['instrument_type']==call_put)
                                            &(self.kite_instruments_book['expiry_datetime']==expiry_datetime)] \
                                            ['strike'].unique()
            available_strikes = list(available_strikes)

            return available_strikes
        if self.broker_for_data== 'KOTAK':
            available_strikes = self.kotak_instruments_book[\
                    (self.kotak_instruments_book['instrumentName']==underlying)\
                    &(self.kotak_instruments_book['optionType']==call_put)\
                    &(self.kotak_instruments_book['expiry_datetime']==expiry_datetime)]\
                        ['strike'].unique()
            available_strikes = list(available_strikes)

            return available_strikes
        if self.broker_for_data== 'SIM':
            #GET AVAILABLE STRIKES FROM BACKTEST
            available_strikes = self.sim.instruments_book[(\
                self.sim.instruments_book['underlying']==underlying) 
                & (self.sim.instruments_book['call_put']==call_put)
                &(self.sim.instruments_book['expiry_datetime']==expiry_datetime)] \
                ['strike'].unique()
            available_strikes = list(available_strikes)

            return available_strikes


    @keep_log()
    def is_order_complete (self, broker_order_id) -> Boolean:
        """(T)
        Send confirmation if the order is completed

        Args:
            broker_order_id (str): id of order to be confirmed

        Returns:
            Boolean: True if the order is executed
        """        
        

        if self.broker_for_trade == "ZERODHA":
            order_history = self.kite.order_history(broker_order_id)
            order_complete = False
            if order_history[-1]['status'] == 'COMPLETE':
                order_complete = True

            return order_complete

        elif self.broker_for_trade == 'KOTAK':
            trade_history = self.kotak.trade_report()
            trade_df = pd.DataFrame(trade_history['success'])
            order_complete = False
            if broker_order_id in list(trade_df['orderId'].astype(str)):
                order_complete = True
            
            return order_complete

        #Always return True for Paper trade as orders are not punched
        elif self.broker_for_trade == 'PAPER':
            return True

            

class Exchange:
    def __init__(self) -> None:
        pass

    def set_parameters (self,current_datetime, 
            historical_data_folder_name, underlying_name,
            fno_folder_name,equity_folder_name):
        
        self.underlying_name = underlying_name.upper()
        self.instruments_book = pd.DataFrame()
        self.tick_book = pd.DataFrame()
        
        self.prepare_data_book(current_datetime=current_datetime,
            historical_data_folder_name=historical_data_folder_name,
            fno_folder_name=fno_folder_name,
            equity_folder_name=equity_folder_name)
        
    
    @keep_log(default_return=False)
    def prepare_data_book(self,current_datetime,
            historical_data_folder_name,
            fno_folder_name='FNO',
            equity_folder_name="Equity") -> Boolean:

        # required 
        #      - current_datetime
        #      - historical_data_folder_name
        #      - fno_folder_name
        #      - equity_folder_name

        current_datestring = current_datetime.strftime("%Y-%m-%d")
        
        parent = os.path.dirname(os.getcwd())

        historical_data_folder_path = os.path.join(\
                parent,historical_data_folder_name)
        
        fno_data_folder_path = os.path.join(\
                historical_data_folder_path,fno_folder_name)
        fno_file_paths = [\
            os.path.join(fno_data_folder_path,f) \
            for f in os.listdir(fno_data_folder_path) \
                if os.path.isfile(os.path.join(fno_data_folder_path,f)) \
                    & (f.split(".")[0].split("_")[0]==current_datestring)\
                        ]

        equity_data_folder_path = os.path.join(\
            historical_data_folder_path,equity_folder_name)
        equity_file_paths = [\
            os.path.join(equity_data_folder_path,f) \
            for f in os.listdir(equity_data_folder_path) \
                if os.path.isfile(os.path.join(equity_data_folder_path,f)) \
                    & (f.split(".")[0].split("_")[0]==current_datestring)\
                        ]

        data_file_paths = fno_file_paths
        data_file_paths.extend(equity_file_paths)
        self.tick_book = pd.concat(map(pd.read_csv,data_file_paths))

        self.tick_book.rename(columns={'Ticker':'ticker',
                        'LTP':'ltp',
                        'BuyPrice':'buy_price',
                        'SellPrice':'sell_price'},
                        inplace=True)

        self.tick_book = self.tick_book[['ticker','ltp','buy_price',\
                    'sell_price','underlying','strike','call_put',\
                    'expiry_date','timestamp']]
        self.tick_book = self.tick_book[\
            (self.tick_book['underlying']==self.underlying_name) | \
            (self.tick_book['underlying'].isnull())]
        self.tick_book['expiry_datetime'] = pd.to_datetime(\
                    self.tick_book['expiry_date']) + \
                        timedelta (hours=15, minutes =30)
        self.tick_book['timestamp'] = pd.to_datetime(\
                    self.tick_book['timestamp'])
        df_group = self.tick_book.groupby(['ticker','timestamp'])
        self.tick_book['duplicate_serial'] = df_group[['ltp']].cumcount()
        self.tick_book['duplicate_count'] = df_group['ltp'].transform('size')
        del df_group
        self.tick_book['duplicate_fraction'] = \
            pd.to_timedelta(self.tick_book['duplicate_serial'] \
                / self.tick_book['duplicate_count'], unit='s')
        self.tick_book['timestamp'] = self.tick_book['timestamp'] \
                    + self.tick_book['duplicate_fraction']
        
        self.instruments_book = self.tick_book[['ticker','underlying',\
                    'strike','call_put','expiry_datetime']]

        self.tick_book = self.tick_book[['ticker','ltp','buy_price',\
                    'sell_price','underlying','strike','call_put',\
                    'timestamp']]
        self.instruments_book.drop_duplicates(inplace=True)
        # self.tick_book.to_csv(current_datetime.strftime("interim_df/%Y-%m-%d Tick.csv"), index=False)
        # self.instruments_book.to_csv(current_datetime.strftime("interim_df/%Y-%m-%d Instruments.csv"), index=False)
        return True

    @keep_log()
    def ltp(self,instruments,current_datetime, initiation_time) -> float:
        
        slippage = perf_counter() - initiation_time
        current_datetime_slippage_adj = \
            current_datetime + timedelta(seconds=slippage)
        current_datetime_slippage_adj
        if type(instruments) == pd.DataFrame:
            ltp_df = instruments.copy()

            df_before_current_time = ltp_df.merge(self.tick_book,how='left',left_on='instrument_id_data', right_on='ticker')

            df_before_current_time = df_before_current_time[df_before_current_time['timestamp']\
                <=current_datetime_slippage_adj]\
                    .drop_duplicates(subset='ticker',keep='last')

            ltp_df = ltp_df.merge(df_before_current_time,how='left',left_on='instrument_id_data', right_on='ticker')

            ltp_df = ltp_df[['ticker','ltp']]

            return ltp_df
        else:
 
            k = self.tick_book[(self.tick_book['ticker']==instruments)\
                &(self.tick_book['timestamp']<=current_datetime_slippage_adj)]
            k = k[['ltp']].iloc[-1][0]

            return k

    @keep_log(default_return = {'buy_price':0,'sell_price':0})
    def quote (self,instrument_id,current_datetime, initiation_time):
        #Quote

        slippage = perf_counter() - initiation_time
        current_datetime_slippage_adj = \
            current_datetime + timedelta(seconds=slippage)
        current_datetime_slippage_adj

        quote = self.tick_book[(self.tick_book['ticker']==instrument_id)\
            &(self.tick_book['timestamp']<=current_datetime_slippage_adj)]\
            [['buy_price','sell_price']].iloc[-1]
        return quote


    pass
import pandas as pd
from datetime import datetime, timedelta
import kiteconnect
from kiteconnect import KiteConnect
# import kite_connection
from ks_api_client import ks_api


class Broker:
    def __init__(self, broker_for_trade, broker_for_data,
                kite_api_key=None, kite_access_token=None,
                kotak_consumer_key=None, kotak_access_token=None,
                kotak_consumer_secret=None,kotak_user_id=None,
                kotak_access_code=None, kotak_user_password=None,
                logger=None, current_datetime=None) -> None:
        if current_datetime is None: current_datetime = datetime.now()
        self.broker_for_trade = broker_for_trade.upper()
        self.broker_for_data = broker_for_data.upper()
        self.logger = logger
        self.class_name_dict_for_logger = {'className':self.__class__.__name__}
        self.kite = None
        self.kotak = None
        self.backtest_object = None
        self.kite_instruments_book = None
        self.kotak_instruments_book = None
        self.paper_instruments_book = None
        self.positions_book = pd.DataFrame()
        self.paper_trade_realized_pnl = 0
        self.set_broker_object(broker_name=self.broker_for_trade,
                            current_datetime=current_datetime,
                            kite_api_key = kite_api_key,
                            kite_access_token=kite_access_token,
                            kotak_access_code=kotak_access_code,
                            kotak_consumer_secret=kotak_consumer_secret,
                            kotak_access_token=kotak_access_token,
                            kotak_user_id=kotak_user_id,
                            kotak_user_password=kotak_user_password,
                            kotak_consumer_key=kotak_consumer_key)
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
                            kotak_consumer_key=kotak_consumer_key)


        self.tradebook = pd.DataFrame()

    def set_broker_object(self,broker_name,current_datetime = None,
                            kite_api_key=None, kite_access_token=None,
                            kotak_consumer_key=None, kotak_access_token=None,
                            kotak_consumer_secret=None,kotak_user_id=None,
                            kotak_access_code=None, kotak_user_password=None) -> bool:
        self.logger.info(\
            f"| | |Broker Name -> {broker_name}", \
                extra=self.class_name_dict_for_logger)
        try:
            if broker_name == 'ZERODHA':
                self.kite = KiteConnect(api_key=kite_api_key)
                self.kite.set_access_token(kite_access_token)
                
                self.logger.info(\
                    f"| | |{broker_name} -> Initialized", \
                    extra=self.class_name_dict_for_logger)

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

                # kite_instruments_book = ['tradingsymbol','name','strike',
                #                          'instrument_type',\
                #                          'exchange','expiry',\
                #                          'expiry_datetime']]
                
                self.logger.info(\
                    f"| | |{broker_name} -> Instruments Book Prepared", \
                    extra=self.class_name_dict_for_logger)

                return True
            
            elif broker_name == 'KOTAK':
                
                
                self.kotak = ks_api.KSTradeApi(access_token = kotak_access_token, userid = kotak_user_id, \
                                                consumer_key = kotak_consumer_key, ip = "127.0.0.1", 
                                                app_id = "app_id")

                self.kotak.login(password=kotak_user_password)
                
                a=self.kotak.session_2fa(access_code = kotak_access_code)
                

                self.logger.info(\
                    f"| | |{broker_name} -> Initialized", \
                    extra=self.class_name_dict_for_logger)

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

                # kotak_instruments_book -> ['instrumentToken','instrumentName',
                #                           'strike','optionType','expiry',
                #                           'expiry_datetime']

                self.logger.info(\
                    f"| | |{broker_name} -> Instruments Book Prepared", \
                    extra=self.class_name_dict_for_logger)

                return True

            elif broker_name == 'PAPER':
                self.logger.info(\
                    f"| | |{broker_name} -> Ready", \
                    extra=self.class_name_dict_for_logger)
                return True
            elif broker_name == 'BACKTEST':
                ## INITIALIZE BACKTESTING OBJECT HERE
                return True
        except Exception as e:
            self.logger.info(\
            f'''| | |Broker Name -> Exception setting up Broker Object:{e}''', \
                extra=self.class_name_dict_for_logger)
            print("Exception setting up Broker Object: %s\n" % e)
            return False

    def get_fno_instrument_id (self, broker_for, strike, underlying, call_put, expiry_datetime) -> str:
        
        instrument_id_broker = self.broker_for_trade
        if (broker_for.upper() == 'DATA') | (self.broker_for_trade == 'PAPER'):
            instrument_id_broker = self.broker_for_data

        self.logger.info(\
                    f"| | |{instrument_id_broker} for {broker_for} -> FnO Instrument ID", \
                    extra=self.class_name_dict_for_logger)
        try:
            if instrument_id_broker == 'ZERODHA':  
                
                instrument_id = str(self.kite_instruments_book[(self.kite_instruments_book['name']==underlying)
                        &(self.kite_instruments_book['instrument_type']==call_put)
                        &(self.kite_instruments_book['expiry_datetime']==expiry_datetime)
                        &(self.kite_instruments_book['strike']==strike)]['tradingsymbol'].iloc[0])
                
                if not isinstance(instrument_id,str):
                    instrument_id = str(instrument_id.iloc[0])
                
                self.logger.info(\
                    f"| | |{instrument_id_broker}-{underlying}-{strike}-{call_put}-{expiry_datetime}->{instrument_id}", \
                    extra=self.class_name_dict_for_logger)

                return instrument_id

            elif instrument_id_broker == 'KOTAK':
                
                instrument_id = str(self.kotak_instruments_book[ (self.kotak_instruments_book['strike']==strike)
                    & (self.kotak_instruments_book['instrumentName']==underlying) 
                    & (self.kotak_instruments_book['expiry_datetime']==expiry_datetime)
                    & (self.kotak_instruments_book['optionType']==call_put)]['instrumentToken'].iloc[0])
                
                self.logger.info(\
                    f"| | |{instrument_id_broker}-{underlying}-{strike}-{call_put}-{expiry_datetime}->{instrument_id}", \
                    extra=self.class_name_dict_for_logger)

                return instrument_id
            elif instrument_id_broker== 'BACKTEST':
                instrument_id = None 
                ## GET INSTRUMENT ID FOR BACKTEST
                return instrument_id
        except Exception as e:
            self.logger.info(\
                    f'''| | |Exception: {e}''', \
                    extra=self.class_name_dict_for_logger)
            return None

    def get_multiple_fno_instrument_id (self, broker_for, fno_df) -> pd.Series:
        # fno_df should have underlying, call_put, expiry_datetime and stike column
        
        instrument_id_broker = self.broker_for_trade
        if (broker_for.upper() == 'DATA') | (self.broker_for_trade == 'PAPER'): 
            instrument_id_broker = self.broker_for_data
        
        self.logger.info(\
                    f"| | |{instrument_id_broker} -> Multiple FnO Instrument ID", \
                    extra=self.class_name_dict_for_logger)
        try:
            if instrument_id_broker == 'ZERODHA':  
                instrument_id = fno_df.merge(self.kite_instruments_book,how='left',\
                        left_on=['underlying','call_put','expiry_datetime','strike'], 
                        right_on=['name','instrument_type','expiry_datetime','strike'])\
                        ['tradingsymbol'].astype(str)
                
                self.logger.info(\
                    f"| | |{instrument_id_broker}-{fno_df.to_json()}->{instrument_id.to_json()}", \
                    extra=self.class_name_dict_for_logger)

                return instrument_id

            elif instrument_id_broker == 'KOTAK':
                instrument_id = None 
                ## GET INSTRUMENT ID FOR KOTAK -> COMPLETE
                instrument_id = fno_df.merge(self.kotak_instruments_book,how='left',\
                        left_on=['underlying','call_put','expiry_datetime','strike'], 
                        right_on=['instrumentName','optionType','expiry_datetime','strike'])\
                        ['instrumentToken'].astype(str)
                
                self.logger.info(\
                    f"| | |{instrument_id_broker}-{fno_df.to_json()}->{instrument_id.to_json()}", \
                    extra=self.class_name_dict_for_logger)
                
                return instrument_id
            elif instrument_id_broker == 'BACKTEST':
                instrument_id = None 
                ## GET INSTRUMENT ID FOR BACKTEST
                return instrument_id
        except Exception as e:
            self.logger.info(\
                    f'''| | |Exception: {e}''', \
                    extra=self.class_name_dict_for_logger)
            return None
    
    def place_market_order(self, instrument_id, buy_sell, quantity, exchange='NFO') -> str:

        self.logger.info(\
                f"|{datetime.now().date()}|{datetime.now().time()}|\
                    {self.broker_for_trade}-{instrument_id}-{buy_sell}-{quantity}-{exchange}\
                        -> Place Order".replace(" ",""), \
                    extra=self.class_name_dict_for_logger)
        try:

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

                

                self.logger.info(\
                    f"|{datetime.now().date()}|{datetime.now().time()}|\
                        {self.broker_for_trade}-{instrument_id}-{buy_sell}-{quantity}-{exchange}\
                            - Order Placed-> {broker_order_id}".replace(" ",""), \
                        extra=self.class_name_dict_for_logger)

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

                self.logger.info(\
                    f"|{datetime.now().date()}|{datetime.now().time()}|\
                        {self.broker_for_trade}-{instrument_id}-{buy_sell}-{quantity}-{exchange}\
                            - Order Placed-> {broker_order_id}".replace(" ",""), \
                        extra=self.class_name_dict_for_logger)
                    

                return broker_order_id

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
                    price = 0
                else:
                    price = 0
                

                broker_order_id = str(datetime.now())
                position = {'broker_order_id':broker_order_id,\
                            'instrument_id':instrument_id,\
                            'quantity':quantity*multiplier,\
                            'exchange':exchange,\
                            'average_price':price}
                
                self.positions_book = self.positions_book.append\
                                    (position,ignore_index=True)

                self.get_pnl()

                self.logger.info(\
                    f"|{datetime.now().date()}|{datetime.now().time()}|\
                        {self.broker_for_trade}-{instrument_id}-{buy_sell}-{quantity}-{exchange}\
                            - Order Placed-> {broker_order_id}".replace(" ",""), \
                        extra=self.class_name_dict_for_logger)

                return broker_order_id

            elif self.broker_for_trade == "BACKTEST":
                ## PLACE ORDER ON BACKTEST
                return None

        except Exception as e:
            self.logger.info(\
                    f'''| | |Exception: {e}''', \
                    extra=self.class_name_dict_for_logger)
            return None

    def cancel_order(self, broker_order_id) -> str:

        self.logger.info(\
                f"|{datetime.now().date()}|{datetime.now().time()}|\
                    {self.broker_for_trade}-{broker_order_id}\
                        -> Cancel Order", \
                    extra=self.class_name_dict_for_logger)
        try:

            if self.broker_for_trade == "ZERODHA":   
                # Place an intraday market order on NSE

                broker_order = self.kite.cancel_order(variety=self.kite.VARIETY_REGULAR,\
                                                order_id=broker_order_id)
                                
                order_cancelled = False

                self.logger.info(\
                    f"|{datetime.now().date()}|{datetime.now().time()}|\
                        {self.broker_for_trade}-{broker_order_id}\
                            - Order Placed-> {order_cancelled}".replace(" ",""), \
                        extra=self.class_name_dict_for_logger)

                return order_cancelled

            elif self.broker_for_trade == "KOTAK":
                ## PLACE ORDER ON KOTAK
                cancelled_order = self.kotak.cancel_order(broker_order_id)

                order_cancelled = False
                if list(cancelled_order)[0]=='Success':
                    order_cancelled = True

                self.logger.info(\
                    f"|{datetime.now().date()}|{datetime.now().time()}|\
                        {self.broker_for_trade}-{broker_order_id}\
                            - Order Cancelled-> {order_cancelled}".replace(" ",""), \
                        extra=self.class_name_dict_for_logger)

                return order_cancelled

            elif self.broker_for_trade == "PAPER":
                self.logger.info(\
                f"|{datetime.now().date()}|{datetime.now().time()}|\
                    {self.broker_for_trade}-{broker_order_id}\
                        -> Paper Trade Order Cancelled".replace(" ",""), \
                    extra=self.class_name_dict_for_logger)
                return True

            elif self.broker_for_trade == "BACKTEST":
                ## PLACE ORDER ON BACKTEST
                return None

        except Exception as e:
            self.logger.info(\
                    f'''| | |Exception: {e}''', \
                    extra=self.class_name_dict_for_logger)
            return None

    def get_ltp (self, instrument_id, exchange="NSE") -> float:

        self.logger.info(\
                    f"| | |{self.broker_for_data}-{instrument_id} -> Get LTP", \
                    extra=self.class_name_dict_for_logger)

        try:
            if self.broker_for_data == "ZERODHA":
                if instrument_id == 'NIFTY': instrument_id = "NIFTY 50"
                ltp =  self.kite.ltp(f'{exchange}:{instrument_id}') \
                    [f'{exchange}:{instrument_id}'] \
                    ['last_price']

                self.logger.info(\
                    f"| | |{self.broker_for_data}-{instrument_id}-LTP -> {ltp}", \
                    extra=self.class_name_dict_for_logger)

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

                self.logger.info(\
                    f"| | |{self.broker_for_data}-{instrument_id}-LTP -> {ltp}", \
                    extra=self.class_name_dict_for_logger)

                return ltp
            elif self.broker_for_data == 'BACKTEST':
                ## GET LTP FROM BACKTEST
                pass
        except Exception as e:
            self.logger.info(\
                    f'''| | |Exception: {e}''', \
                    extra=self.class_name_dict_for_logger)
            return None

    def get_multiple_ltp (self, instrument_df, exchange="NFO") -> pd.Series:
        ## instrument_df should have instrument_id_data column

        self.logger.info(\
                    f"| | |{self.broker_for_data}-{instrument_df.to_json()} -> Get Multiple LTP", \
                    extra=self.class_name_dict_for_logger)

        try:
            if self.broker_for_data == "ZERODHA":
                df = instrument_df.copy()
                df['exchange:instrument_id'] = exchange + ":" + df['instrument_id_data']
                last_price = pd.DataFrame.from_dict(self.kite.ltp(\
                                list(df['exchange:instrument_id']))\
                                ,orient='index')
                last_price['exchange:instrument_id'] = last_price.index
                ltp = df.merge(last_price,how='left',on='exchange:instrument_id')['last_price']

                self.logger.info(\
                    f"| | |{self.broker_for_data}-{instrument_df.to_json()}-LTP -> {ltp.to_json()}", \
                    extra=self.class_name_dict_for_logger)

                return ltp

            elif self.broker_for_data == "KOTAK":
                ## GET LTP FROM KOTAK
                df = instrument_df.copy()
                df['ltp'] = None
                for idx,each_instrument in df.iterrows():
                    instrument_id = each_instrument['instrument_id_data']
                    df.loc[idx,'ltp'] = self.get_ltp(instrument_id=instrument_id)
                
                ltp = df['ltp']

                self.logger.info(\
                    f"| | |{self.broker_for_data}-{instrument_df.to_json()}-LTP -> {ltp.to_json()}", \
                    extra=self.class_name_dict_for_logger)

                return ltp

            elif self.broker_for_data == 'BACKTEST':
                ## GET LTP FROM BACKTEST
                pass
        except Exception as e:
            self.logger.info(\
                    f'''| | |Exception: {e}''', \
                    extra=self.class_name_dict_for_logger)
            return None

    def get_positions (self) -> pd.DataFrame:
        
        self.logger.info(\
                    f"| | |{self.broker_for_trade} -> Get Positions", \
                    extra=self.class_name_dict_for_logger)

        try:
            if self.broker_for_trade == 'ZERODHA':
                positions = pd.DataFrame(self.kite.positions()['net'])
                positions.rename(columns={'tradingsymbol':'instrument_id_trade'}, inplace=True)

                self.logger.info(\
                    f"| | |{self.broker_for_trade}-Positions -> {positions.to_json()}", \
                    extra=self.class_name_dict_for_logger)

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

                self.logger.info(\
                    f"| | |{self.broker_for_trade}-Positions -> {positions_df.to_json()}", \
                    extra=self.class_name_dict_for_logger)

                return positions_df

            elif self.broker_for_trade == 'PAPER':
                positions_df = self.positions_book.copy()
                positions_df['instrument_id_trade'] = positions_df['instrument_id']
                positions_df = positions_df[['instrument_id_trade','quantity','exchange']]

                self.logger.info(\
                    f"| | |{self.broker_for_trade}-Positions -> {positions_df.to_json()}", \
                    extra=self.class_name_dict_for_logger)

                return positions_df
                
            elif self.broker_for_trade == 'BACKTEST':
                ## GET PNL FROM BACKTEST
                return None
        except Exception as e:
            self.logger.info(\
                    f'''| | |Exception: {e}''', \
                    extra=self.class_name_dict_for_logger)
            return None
    
    def get_pnl (self) -> float:

        self.logger.info(\
                    f"| | |{self.broker_for_trade} -> Get PnL", \
                    extra=self.class_name_dict_for_logger)
        try:
            if self.broker_for_trade == 'ZERODHA':
                positions = pd.DataFrame(self.kite.positions()['net'])
                if len(positions)==0:
                    pnl = 0
                else:
                    pnl =  round(positions['pnl'].sum(),2)

                self.logger.info(\
                    f"| | |{self.broker_for_trade}-PnL->{pnl}", \
                    extra=self.class_name_dict_for_logger)

                return pnl
            elif self.broker_for_trade == 'KOTAK':
               
                positions = self.kotak.positions(position_type='TODAYS')
                self.logger.info(\
                    f"| | |{self.broker_for_trade}-Positions Obtained", \
                    extra=self.class_name_dict_for_logger)
                pnl=0
                if list(positions)[0] == 'Success':
                    positions_df = pd.DataFrame(positions['Success'])
                    if len(positions_df) != 0:
                        positions_df['unrealized_pnl'] = positions_df['netTrdQtyLot']\
                                                         * positions_df['lastPrice']
                        pnl = positions_df['sellTradedVal'].sum()\
                                - positions_df['buyTradedVal'].sum()\
                                + positions_df['unrealized_pnl'].sum()

                self.logger.info(\
                    f"| | |{self.broker_for_trade}-PnL->{pnl}", \
                    extra=self.class_name_dict_for_logger)


                return pnl

            elif self.broker_for_trade == 'PAPER':
                if len(self.positions_book) == 0:
                    self.logger.info(\
                    f"| | |{self.broker_for_trade}-PnL->0", \
                    extra=self.class_name_dict_for_logger)
                    
                    return self.paper_trade_realized_pnl
                self.positions_book['instrument_id_data'] = \
                                        self.positions_book['instrument_id']
                self.positions_book['ltp'] = self.get_multiple_ltp\
                                            (self.positions_book,\
                                            exchange='NFO')

                self.positions_book['price_change'] = self.positions_book['ltp']\
                                         - self.positions_book['average_price']
                self.positions_book['pnl'] = self.positions_book['price_change']\
                                            * self.positions_book['quantity']

                pnl = round(self.paper_trade_realized_pnl \
                    + self.positions_book['pnl'].sum(),2)

                self.positions_book = self.positions_book[['instrument_id',\
                                    'exchange','quantity','ltp',\
                                    'pnl']]\
                                    .groupby(['instrument_id',\
                                        'exchange','ltp'])\
                                    .sum().reset_index()

                self.paper_trade_realized_pnl += round(self.positions_book[\
                                self.positions_book['quantity']==0]\
                                ['pnl'].sum(),2)

                self.positions_book = self.positions_book[\
                                self.positions_book['quantity']!=0]

                self.positions_book['average_price'] = self.positions_book['ltp'] - \
                            (\
                                self.positions_book['pnl']\
                             / self.positions_book['quantity']
                             )

                self.positions_book.to_csv(f'''interim_df/positions\
                        {datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}.csv'''\
                        .replace(" ","")
                        , index=False)

                self.logger.info(\
                    f"| | |{self.broker_for_trade}-PnL->{pnl}-{self.positions_book}", \
                    extra=self.class_name_dict_for_logger)

                return pnl
                
            elif self.broker_for_trade == 'BACKTEST':
                ## GET PNL FROM BACKTEST
                return None
        except Exception as e:
            self.logger.info(\
                    f'''| | |Exception: {e}''', \
                    extra=self.class_name_dict_for_logger)
            return None

    def get_next_expiry_datetime(self, underlying='NIFTY') -> datetime:

        self.logger.info(\
                    f"| | |{self.broker_for_data}-{underlying} -> Get Next Expiry", \
                    extra=self.class_name_dict_for_logger)
        try:
            if self.broker_for_data == 'ZERODHA':
                next_expiry_datetime = self.kite_instruments_book[\
                                        (self.kite_instruments_book['name']==underlying.upper()) 
                                        & (self.kite_instruments_book['instrument_type'] == 'CE')]\
                                        ['expiry_datetime'].min()

                self.logger.info(\
                    f"| | |{self.broker_for_data}-{underlying}-Next Expiry->{next_expiry_datetime}", \
                    extra=self.class_name_dict_for_logger)

                return next_expiry_datetime

            elif self.broker_for_data == 'KOTAK':
                next_expiry_datetime = self.kotak_instruments_book[(\
                                    self.kotak_instruments_book['instrumentName']==underlying.upper())\
                                & (self.kotak_instruments_book['optionType']=='CE')]\
                                ['expiry_datetime'].min()

                self.logger.info(\
                    f"| | |{self.broker_for_data}-{underlying}-Next Expiry->{next_expiry_datetime}", \
                    extra=self.class_name_dict_for_logger)
                
                return next_expiry_datetime

            elif self.broker_for_data == 'PAPER TRADE':
                ## GET NEXT EXPIRY DATE FROM KOTAK
                return None
        except Exception as e:
            self.logger.info(\
                    f'''| | |Exception: {e}''', \
                    extra=self.class_name_dict_for_logger) 
            return None

    def get_available_strikes (self, underlying, call_put, expiry_datetime):
        
        self.logger.info(\
                    f"| | |{self.broker_for_data}-{underlying}-{call_put}-{expiry_datetime}\
                        -> Get Available Strikes".replace(" ",""), \
                    extra=self.class_name_dict_for_logger)
        try:
            if self.broker_for_data == 'ZERODHA':
                available_strikes = self.kite_instruments_book[(self.kite_instruments_book['name']==underlying) 
                                                & (self.kite_instruments_book['instrument_type']==call_put)
                                                &(self.kite_instruments_book['expiry_datetime']==expiry_datetime)] \
                                                ['strike'].unique()
                available_strikes = list(available_strikes)

                self.logger.info(\
                    f"| | |{self.broker_for_data}-{underlying}-{call_put}-{expiry_datetime}\
                        -Get Available Strikes -> {available_strikes}".replace(" ",""), \
                    extra=self.class_name_dict_for_logger)

                return available_strikes
            if self.broker_for_data== 'KOTAK':
                available_strikes = self.kotak_instruments_book[\
                        (self.kotak_instruments_book['instrumentName']==underlying)\
                        &(self.kotak_instruments_book['optionType']==call_put)\
                        &(self.kotak_instruments_book['expiry_datetime']==expiry_datetime)]\
                            ['strike'].unique()
                available_strikes = list(available_strikes)

                self.logger.info(\
                    f"| | |{self.broker_for_data}-{underlying}-{call_put}-{expiry_datetime}\
                        -Get Available Strikes -> {available_strikes}".replace(" ",""), \
                    extra=self.class_name_dict_for_logger)

                return available_strikes
            if self.broker_for_data== 'BACKTEST':
                #GET AVAILABLE STRIKES FROM BACKTEST
                return None

        except Exception as e:
            self.logger.info(\
                    f'''| | |Exception: {e}''', \
                    extra=self.class_name_dict_for_logger)
            return None

    def is_order_complete (self, broker_order_id):
        self.logger.info(\
                    f"| | |{self.broker_for_trade}-{broker_order_id} -> Is Order Complete", \
                    extra=self.class_name_dict_for_logger)
        try:
            if self.broker_for_trade == "ZERODHA":
                order_history = self.kite.order_history(broker_order_id)
                order_complete = False
                if order_history[-1]['status'] == 'COMPLETE':
                    order_complete = True

                self.logger.info(\
                    f"| | |{self.broker_for_trade}-{broker_order_id}-Order Complete? -> \
                        {order_history[-1]['status']}".replace(" ",""), \
                    extra=self.class_name_dict_for_logger)

                return order_complete
            elif self.broker_for_trade == 'KOTAK':
                trade_history = self.kotak.trade_report()
                trade_df = pd.DataFrame(trade_history['success'])
                order_complete = False
                if broker_order_id in list(trade_df['orderId'].astype(str)):
                    order_complete = True
                self.logger.info(\
                    f"| | |{self.broker_for_trade}-{broker_order_id}-Order Complete? -> \
                        {order_complete}".replace(" ",""), \
                    extra=self.class_name_dict_for_logger)
                
                return order_complete

            elif self.broker_for_trade == 'PAPER':
                self.logger.info(\
                    f"| | |{self.broker_for_trade}-{broker_order_id}-Order Complete? -> \
                        Paper Trade".replace(" ",""), \
                    extra=self.class_name_dict_for_logger)
                return True
            elif self.broker_for_trade == 'BACKTEST':
                ## ADD ORDER VERIFICATION for BACKTEST
                return False
        except Exception as e:
            self.logger.info(\
                    f'''| | |Exception: {e}''', \
                    extra=self.class_name_dict_for_logger)
            return False

            

class Exchange:



    pass
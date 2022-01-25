import pandas as pd
import numpy as np
from time import perf_counter, perf_counter_ns
from time import sleep
from datetime import datetime as dt
from datetime import timedelta
import datetime
# import datetime
import random
from py_vollib.black_scholes.implied_volatility import implied_volatility as bs_iv
from py_vollib.black_scholes.greeks import analytical as greeks
import time
import ks_api_client
from kiteconnect import KiteConnect
from kiteconnect import KiteTicker

tokens = []
def set_kite (kite_api_key=None, kite_access_token=None):

    kite = KiteConnect(api_key=kite_api_key)
    kite.set_access_token(kite_access_token)
    kws = KiteTicker(kite_api_key,kite_access_token)
    return kite, kws


def get_instruments_book (kite):
    nse = pd.DataFrame(kite.instruments("NSE"))
    nfo = pd.DataFrame(kite.instruments("NFO"))
    kite_instruments_book = nfo.append(nse)
    kite_instruments_book['expiry'] =  pd.to_datetime(kite_instruments_book['expiry'], 
                                            format='%Y-%m-%d') 
    kite_instruments_book['expiry_datetime'] = kite_instruments_book['expiry'] + \
                                        timedelta (hours=15, minutes =30)
    return kite_instruments_book


def tokenLookup(instrument_df,symbol_list):
    """Looks up instrument token for a given script from instrument dump"""
    token_list = []
    for symbol in symbol_list:
        token_list.append(int(instrument_df[instrument_df.tradingsymbol==symbol].instrument_token.values[0]))
    return token_list


def get_fno_instrument_id (instruments_df,strike, underlying, call_put, expiry_datetime) -> str:
        
            
    instrument_id = str(instruments_df[(instruments_df['name']==underlying)
            &(instruments_df['instrument_type']==call_put)
            &(instruments_df['expiry_datetime']==expiry_datetime)
            &(instruments_df['strike']==strike)]['instrument_token'].iloc[0])
    
    if not isinstance(instrument_id,str):
        instrument_id = str(instrument_id.iloc[0])

    return instrument_id


def get_multiple_fno_instrument_id (instruments_df, fno_df) -> pd.Series:
        # fno_df should have underlying, call_put, expiry_datetime and strike column

    instrument_id = fno_df.merge(instruments_df,how='left',\
            left_on=['underlying','call_put','expiry_datetime','strike'], 
            right_on=['name','instrument_type','expiry_datetime','strike'])\
            ['instrument_token'].astype(int)

    return instrument_id

def on_close(ws, code, reason):
    print("closed connection on close: {} {}".format(code, reason))


def on_error(ws, code, reason):
    print("closed connection on error: {} {}".format(code, reason))


def on_noreconnect(ws):
    print("Reconnecting the websocket failed")


def on_reconnect(ws, attempt_count):
    print("Reconnecting the websocket: {}".format(attempt_count))


def on_order_update(ws, data):
    print("order update: ", data)


def on_connect(ws,response):
    global tokens
    print("on connect: {}".format(response))
    # Callback on successful connect.
    # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).
    #print("on connect: {}".format(response))
    ws.subscribe(tokens)
    ws.set_mode(ws.MODE_FULL,tokens) # Set all token tick in `full` mode.
    #ws.set_mode(ws.MODE_FULL,[tokens[0]])  # Set one token tick in `full` mode.

counter = 0
combined_df = pd.DataFrame()
def on_ticks(ws,ticks):
    # Callback to receive ticks.
    # print("Ticks: {}".format(ticks))
    # print(ticks)
    # populate_df.delay(ticks)
    global counter
    global combined_df
    counter +=1
    tick_df = pd.DataFrame(ticks)[['instrument_token',\
        'last_price','volume','buy_quantity',\
        'sell_quantity','oi','timestamp','depth']]
    combined_df = combined_df.append(tick_df)
    # if dt.now().second == 0:
    #     combined_df.to_csv("tick_stream.csv",sep="|")
    #     print(f"saved,{counter}",end='\r')
    # else:
    print(f"         {counter}",end='\r')

def populate_df (ticks):
    global counter
    global combined_df
    counter +=1
    tick_df = pd.DataFrame(ticks)[['instrument_token',\
        'last_price','volume','buy_quantity',\
        'sell_quantity','oi','timestamp','depth']]
    combined_df = combined_df.append(tick_df)
    # if dt.now().second == 0:
    #     combined_df.to_csv("tick_stream.csv",sep="|")
    #     print(f"saved,{counter}",end='\r')
    # else:
    print(f"         {counter}",end='\r')


kite, kws = set_kite(kite_api_key="5ytarhiur9g1jebq",
    kite_access_token="oAwZpfJqmnb33FKt5wEczWCZmSdaTMKs")
instruments_df = get_instruments_book(kite)

next_expiry_datetime = instruments_df[\
                                    (instruments_df['name']=='NIFTY') 
                                    & (instruments_df['instrument_type'] == 'CE')]\
                                    ['expiry_datetime'].min()


# names = ['NIFTY']
names = ['NIFTY 50', 'INDIA VIX','NIFTY BANK', 'NIFTY','BANKNIFTY']
# names = ['NIFTY 50', 'INDIA VIX','NIFTY BANK']

tokens = list(instruments_df[(instruments_df['name']\
        .isin(names))&(instruments_df['expiry_datetime']==next_expiry_datetime)]\
            ['instrument_token'].astype(int))
print(type(tokens))
print(len(tokens))
# strike = 18_000.0
# underlying = 'NIFTY'
# expiry_datetime=dt(2022,1,27,15,30)
# fno_df = pd.DataFrame({'strike':[strike,strike,strike],\
#                         'underlying':[underlying,underlying,underlying],
#                         'expiry_datetime':[expiry_datetime,expiry_datetime,expiry_datetime],
#                         'call_put':['CE','PE','PE']})
# fno_df['tokens'] = get_multiple_fno_instrument_id(
#         instruments_df=instruments_df,
#         fno_df=fno_df)
# tokens = list(fno_df['tokens'])
# print(tokens)

# Assign the callbacks.
combined_df = pd.DataFrame()
kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close
kws.on_error = on_error
kws.on_noreconnect = on_noreconnect
kws.on_reconnect = on_reconnect
kws.on_order_update = on_order_update

# kws.enable_reconnect(reconnect_interval=5, reconnect_tries=50)

# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.
# kws.connect(disable_ssl_verification=True)
kws.connect()
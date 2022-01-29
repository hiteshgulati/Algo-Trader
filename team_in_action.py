from algo_module import Algo_manager
from datetime import datetime
from time import perf_counter
from time import sleep
import json
import os
import warnings
from kiteconnect import KiteConnect
warnings.filterwarnings("ignore")

def generate_access_token(broker_secret, request_token) -> str:

        kite = KiteConnect(api_key=broker_secret['kite_api_key'])

        data = kite.generate_session(request_token=request_token, \
            api_secret=broker_secret['kite_api_secret'])
        print("Token Generated")
        del kite 

        return data['access_token']

if __name__ == '__main__':
    
    # start_time = datetime(2020,1,1,9,28).time()
    # close_time = datetime(2020,1,1,15,7).time()
    # end_time = datetime(2020,1,1,15,10,0).time()

    start_time = datetime(2020,1,1,2,8).time()
    close_time = datetime(2020,1,1,2,31).time()
    end_time = datetime(2020,1,1,2,31,30).time()

    non_expiry_day_no_candle_time = datetime(2020, 1, 1, 14, 30).time()
    expiry_day_no_candle_time = datetime(2020, 1, 1, 13, 0).time()


    is_kite_access_token_available = True

    kite_request_token='cVcYOdYyZbhD8p12gUs72xbxYGkN8tJN'

    parent = os.path.dirname(os.getcwd())
    file_path = os.path.join(parent,'broker_secret.json')

    with open (file_path, "r") as openfile:
        broker_secret = json.load(openfile)

    if not is_kite_access_token_available:
        broker_secret['kite_access_token'] = \
            generate_access_token(broker_secret,kite_request_token)

        with open (file_path, "w") as outfile:
            json.dump(broker_secret,outfile)

    # logs_folder_path = os.path.join(parent,'logs')
    logs_folder_path = os.path.join(os.getcwd(),'logs')

    algo_manager = Algo_manager(broker_for_trade='paper',
                    broker_for_data='zerodha',
                    per_trade_fee = -.01,
                    underlying_name='NIFTY',
                    kotak_consumer_key=broker_secret['kotak_consumer_key'],
                    kotak_access_token=broker_secret['kotak_access_token'],
                    kotak_consumer_secret=broker_secret['kotak_consumer_secret'],
                    kotak_user_id=broker_secret['kotak_user_id'],
                    kotak_access_code=broker_secret['kotak_access_code'],
                    kotak_user_password=broker_secret['kotak_user_password'],
                    kite_api_key=broker_secret['kite_api_key'],
                    kite_access_token=broker_secret['kite_access_token'],
                    log_folder=logs_folder_path,
                    begin_time=start_time,
                    close_time=close_time,
                    quantity_per_lot = 50,
                    lots_traded = 10,
                    total_loss_limit_per_lot = -1_500,
                    max_trailing_loss_non_expiry_per_lot = -250,
                    max_trailing_loss_expiry_per_lot = -200,
                    trailing_loss_trigger_per_lot = 1_500,
                    non_expiry_day_no_candle_time = non_expiry_day_no_candle_time,
                    expiry_day_no_candle_time = expiry_day_no_candle_time,
                    candle_length=1)
    
    count = 0
    t0 = perf_counter()
    while datetime.now().time() <= end_time:
        algo_manager.action()
        timestamp_sec = datetime.now().time().strftime("%H:%M:%S")
        time_elapsed = round((perf_counter()-t0)/60,2)
        try:
            iterations_per_minute = round(count/(time_elapsed),0)
        except ZeroDivisionError:
            iterations_per_minute = 0
        # print(f"{iterations_per_minute}-{time_elapsed}-{timestamp_sec}",end='\r')
        print(algo_manager.events_and_actions.display_string(),end='\r')
        count += 1
        sleep(.7)
    print(f"Total Time: {time_elapsed}, Iterations: {count}, Per Minute: {iterations_per_minute}")
    print(f"Days: Profit : {algo_manager.data_guy.strategy_pnl} + {algo_manager.data_guy.brokerage_pnl}")
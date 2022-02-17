from algo_module import Algo_manager
from datetime import datetime, timedelta
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


def execute_algo (**kwargs):

    parent = os.path.dirname(os.getcwd())
    file_path = os.path.join(parent,kwargs['broker_secret_file_name'])

    with open (file_path, "r") as openfile:
        broker_secret = json.load(openfile)

    if kwargs['broker_for_data'] == 'zerodha' or kwargs['broker_for_trade'] == 'zerodha':
        if not kwargs['is_kite_access_token_available']:
            broker_secret['kite_access_token'] = \
                generate_access_token(broker_secret,kwargs['kite_request_token'])

        with open (file_path, "w") as outfile:
            json.dump(broker_secret,outfile)

    logs_folder_path = kwargs['log_folder_name']
    # logs_folder_path = os.path.join(parent,kwargs['log_folder_name'])

    if kwargs['broker_for_data'].upper() == 'SIM':
        current_datetime = kwargs['day_start_datetime']
    else: 
        current_datetime = datetime.now()

    module_initiation_time = datetime.now()
    algo_manager = Algo_manager(
        broker_for_trade=kwargs['broker_for_trade'],
        broker_for_data=kwargs['broker_for_data'],
        per_trade_fee = kwargs['per_trade_fee'],
        underlying_name=kwargs['underlying_name'],
        kotak_consumer_key=broker_secret['kotak_consumer_key'],
        kotak_access_token=broker_secret['kotak_access_token'],
        kotak_consumer_secret=broker_secret['kotak_consumer_secret'],
        kotak_user_id=broker_secret['kotak_user_id'],
        kotak_access_code=broker_secret['kotak_access_code'],
        kotak_user_password=broker_secret['kotak_user_password'],
        kite_api_key=broker_secret['kite_api_key'],
        kite_access_token=broker_secret['kite_access_token'],
        log_folder=logs_folder_path,
        current_datetime = current_datetime,
        broker_connection_loss = kwargs['broker_connection_loss'],
        exchange_connection_loss = kwargs['exchange_connection_loss'],
        begin_time=kwargs['trading_start_time'],
        close_time=kwargs['trading_close_time'],
        quantity_per_lot = 50,
        lots_traded = kwargs['lots_traded'],
        total_loss_limit_per_lot = -1_500,
        max_trailing_loss_non_expiry_per_lot = -250,
        max_trailing_loss_expiry_per_lot = -200,
        trailing_loss_trigger_per_lot = 1_500,
        non_expiry_day_no_candle_time = kwargs['non_expiry_day_no_candle_time'],
        expiry_day_no_candle_time = kwargs['expiry_day_no_candle_time'],
        candle_length=kwargs['candle_length'],
        historical_data_folder_name = kwargs['historical_data_folder_name'],
        fno_folder_name = kwargs['fno_folder_name'],
        equity_folder_name = kwargs['equity_folder_name']
        )
    print(f'Module Initiation took: {datetime.now()-module_initiation_time}')
    count = 0
    execution_start_time = datetime.now()

    if kwargs['broker_for_data'].upper() == 'SIM':
        current_datetime = kwargs['day_start_datetime']
    else: 
        current_datetime = datetime.now()

    while current_datetime.time() <= kwargs['switch_off_time']:
        initiation_time = perf_counter()
        algo_manager.action(current_datetime=current_datetime,
            initiation_time = initiation_time)

        print(current_datetime.strftime('%Y-%b-%d>%I:%M:%S %p, %a                '),
            algo_manager.events_and_actions.display_string(),end='\r')
        
        count += 1
        
        if kwargs['broker_for_data'].upper() == 'SIM':
            slippage = perf_counter() - initiation_time

            current_datetime = current_datetime \
                + timedelta(\
                    seconds=(slippage+\
                        kwargs['pause_between_iterations'])
                )
        else: 
            sleep(kwargs['pause_between_iterations'])
            current_datetime = datetime.now()
    
    time_elapsed = datetime.now() - execution_start_time
    iterations_per_minute = round(count/(time_elapsed.seconds/60),0)
    print(f"Total Time: {time_elapsed}, Iterations: {count}, Per Minute: {iterations_per_minute}")
    
    if kwargs['broker_for_data'].upper() == 'SIM':
        time_elapsed = current_datetime - kwargs['day_start_datetime']
        iterations_per_minute = round(count/(time_elapsed.seconds/60),0)
        print(f"Simulated Time: {time_elapsed}, Iterations: {count}, Per Simulated Minute: {iterations_per_minute}")

    print(f"Days: Profit : {algo_manager.data_guy.strategy_pnl} + {algo_manager.data_guy.brokerage_pnl}")
    pass


if __name__ == '__main__':
    
    #For Live Trading
    day_start_datetime = None
    trading_start_time = datetime(2020,1,1,14,15).time()
    trading_close_time = datetime(2020,1,1,15,7).time()
    switch_off_time = datetime(2020,1,1,15,10,0).time()

    #For Simulation
    # day_start_datetime = datetime(2021,5,17,9,15)
    # trading_start_time = datetime(2021,5,17,9,20).time()
    # trading_close_time = datetime(2021,5,17,13,30).time()
    # switch_off_time =    datetime(2021,5,17,13,37).time()

    # For Live Paper trade
    # day_start_datetime = None
    # trading_start_time = datetime(2020,1,1,12,0).time()
    # trading_close_time = datetime(2020,1,1,15,7).time()
    # switch_off_time =    datetime(2020,1,1,15,10).time()

    # non_expiry_day_no_candle_time = datetime(2020, 1, 1, 9, 15).time()
    non_expiry_day_no_candle_time = datetime(2020, 1, 1, 14, 30).time()
    expiry_day_no_candle_time = datetime(2020, 1, 1, 13, 0).time()

    is_kite_access_token_available = True
    kite_request_token='z9K4YBOwkVirM03BVI17ddAfR8xTR314'

    broker_secret_file_name = 'broker_secret.json'

    log_folder_name = 'logs'

    candle_length = 5
    per_trade_fee = -20
    lots_traded = 2
    underlying_name = 'NIFTY'

    broker_for_trade = 'zerodha'
    broker_for_data = 'zerodha'

    pause_between_iterations = .7 

    broker_connection_loss = None
    exchange_connection_loss = None


    # broker_connection_loss = [{'start_datetime':datetime(2021,5,17,9,16),
    #                             'end_datetime':datetime(2021,5,17,9,18)},
    #                         {'start_datetime':datetime(2021,5,17,9,21),
    #                         'end_datetime':datetime(2021,5,17,9,32)},
    #                         {'start_datetime':datetime(2021,5,17,10,22),
    #                         'end_datetime':datetime(2021,5,17,10,23)},
    #                         {'start_datetime':datetime(2021,5,17,11,32),
    #                         'end_datetime':datetime(2021,5,17,12,50)}]

    

    historical_data_folder_name = 'historical data'
    fno_folder_name = 'FNO'
    equity_folder_name = 'Equity'

    execute_algo (day_start_datetime=day_start_datetime,
        trading_start_time=trading_start_time,
        trading_close_time = trading_close_time,
        switch_off_time = switch_off_time,
        non_expiry_day_no_candle_time = non_expiry_day_no_candle_time,
        expiry_day_no_candle_time = expiry_day_no_candle_time,
        is_kite_access_token_available = is_kite_access_token_available,
        kite_request_token = kite_request_token,
        broker_secret_file_name = broker_secret_file_name,
        log_folder_name = log_folder_name,
        candle_length = candle_length,
        per_trade_fee = per_trade_fee,
        lots_traded = lots_traded,
        broker_for_trade = broker_for_trade,
        broker_for_data = broker_for_data,
        underlying_name = underlying_name,
        pause_between_iterations = pause_between_iterations,
        historical_data_folder_name = historical_data_folder_name,
        fno_folder_name = fno_folder_name,
        equity_folder_name = equity_folder_name,
        broker_connection_loss = broker_connection_loss,
        exchange_connection_loss = exchange_connection_loss)
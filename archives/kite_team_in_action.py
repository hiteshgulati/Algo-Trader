from numpy.core.defchararray import index
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')
import datetime
import time
import team_kite
from kiteconnect import KiteConnect
import kite_connection
import os


start_time = time.perf_counter()

# Set token folder
token_folder_path = "/Users/hg/OneDrive/Code/zerodha tokens"

# Generate Access Token
new_access_token_required = False
if new_access_token_required:
    kite_connection.generate_access_token(request_token="T5RvGruQhNf0SRdmLn8G1NnnVenQ9s7a",
                        token_folder_path=token_folder_path)
    

# Generate trading session
access_token = open(os.path.join(token_folder_path,"access_token.txt"),'r').read()
key_secret = open(os.path.join(token_folder_path,"api_key.txt"),'r').read().split()
kite = KiteConnect(api_key=key_secret[0])
kite.set_access_token(access_token)
print("Kite Connection Established")

# instruments.to_excel('ins.xlsx',index=False)


k1 = team_kite.Knowledge_guy(kite_object=kite)

d1 = team_kite.Data_guy(index_symbol='NIFTY 50',kite_object=kite,knowledge_guy=k1)

a1 = team_kite.Algo_analyst(algo_name='Short Straddle', index_symbol='NIFTY', knowledge_guy=k1, max_rupee_loss= -2000, close_time = datetime.time(15,25,0))

b1 = team_kite.Bookkeeper(kite_object=kite)

t1 = team_kite.PaperTrader(kite_object=kite, knowledge_guy=k1, data_guy=d1)

print(f"Team Ready in: {round(time.perf_counter()-start_time,0)} seconds")

number_of_steps = 5000
counter_start_time = time.perf_counter()

for step in range(number_of_steps):
    
    underlying_ltp = d1.get_index_ltp()
    pnl = b1.pnl
    a1.analyse_risk(pnl=pnl, trader_object=t1, underlying_ltp=underlying_ltp)
    a1.check_time(trader_object=t1)
    a1.analyse_data(underlying_ltp=underlying_ltp)
    build_string = "\t\t\to\t\t\t\t"
    if not a1.active:
        build_string = "Not Active"
    if a1.build['strategy'] == 'straddle':
        build_string = f"{a1.build['low']} ^ {a1.build['high']}"
    elif a1.build['strategy'] == 'strangle':
        build_string = f"{a1.build['low']} - {a1.build['high']}"

    print(f"""{step}\t{datetime.datetime.now().strftime("%H:%M:%S")} - NIFTY @ {round(underlying_ltp,2)}\t- Build -> {build_string}""",end='\r')
    current_orderbook = a1.get_orderbook()
    t1.start_trading(current_orderbook=current_orderbook)
    time.sleep(.5)

end_time = time.perf_counter()

print(f"{number_of_steps} steps completed in {round((end_time-counter_start_time)/60,2)} minutes")

print(f"Seconds per Step:\t{round((end_time-counter_start_time)/number_of_steps,2)}")

print("Done with the Sprint!")


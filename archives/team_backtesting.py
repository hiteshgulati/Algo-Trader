import pandas as pd
import warnings
warnings.filterwarnings('ignore')
import datetime
import logging

logger = logging.getLogger(__name__)

def initialize_logger (simulation_date, log_folder = "Backtesting Logs"):

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(levelname)s|%(className)s->%(funcName)s|%(message)s')

    timestamp_string = datetime.datetime.now().strftime("%Y-%m-%d %H_%M_%S")
    file_handler = logging.FileHandler(f'./{log_folder}/{simulation_date} @ {timestamp_string}kite_team.log')
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

class Data_guy:
    """Data Guy has the responsibility to fetch metrics like Price/Volume from Market
    """
    
    def __init__(self, underlying_name, underlying_df, options_strike_step = 50,
        options_strike_range_start=20_000,options_strike_range_end=40_000) -> None:
    
        self.underlying_name = underlying_name.upper()
        self.underlying_ltp_variable = None
        self.underlying_df = underlying_df
        self.options_strike_range = range(options_strike_range_start,options_strike_range_end+options_strike_step, options_strike_step)
        self.class_name_dict_for_logger = {'className':self.__class__.__name__}
    
        print("Data Guy is Ready")

    def underlying_ltp (self, timestamp) -> int:
        """This function is used to update the underlying_ltp value for the timestamp passed

        Args:
            timestamp (datetime): Timestamp for which we need to update the ltp

        Returns:
            int: underlying_ltp Last Traded Price of the Underlying
        """
        try:
            self.underlying_ltp_variable = self.underlying_df[self.underlying_df['timestamp']<=timestamp]['LTP'].iloc[-1]
        except IndexError:
            pass
        return self.underlying_ltp_variable
  

class Algo_analyst:
    """Algo analyst has th responsibility to 
        a) Analyse PnL risk - Strategy Independant
        b) Check Time bounds - Strategy Independant
        c) Analyse Underlying movement Risk - Based on Strategy
        d) Monitor LTP to find opportuniries and move legs - Core of the Strategy
    """
    
    def __init__(self,algo_name,underlying_name='NIFTY', max_rupee_loss = -2000, 
                    entrytime = datetime.time(9,30,0), exittime = datetime.time(15,20,0),
                    options_strike_step = 50, tradelot = 150, curve_flatter = 4) -> None:
        self.orderbook = []
        self.current_orderbook = []
        self.underlying_name = underlying_name
        self.algo_name = algo_name
        self.build = {'strategy':'away','low':0,'high':0}
        self.underlying_options_step = options_strike_step
        self.active = False
        self.max_rupee_loss = max_rupee_loss
        self.tradelot = tradelot
        self.timestamp = None
        self.entrytime = entrytime
        self.exittime = exittime
        self.curve_flatter = curve_flatter
        self.done_for_the_day = False
        if self.max_rupee_loss >0: self.max_rupee_loss *= -1 # if max rupee loss was entered in positive 
        self.class_name_dict_for_logger = {'className':self.__class__.__name__}
   
        print("Analyst is Ready")

    def get_away_from_market (self, stay_active=True, is_done_for_the_day = False):
        """This method will be called when Analyst wants to get away from of the market for some time or out for the day.

        Args:
            stay_active (bool, optional): True: Analyst will stay active after closing all positions, False: Analyst will cease to be active. Defaults to True.
        """
        if self.active:
            self.enter_orders(instrument={'strike':self.build['high'],'call_put':'PE'}, 
                                     buy_sell='buy',quantity = self.tradelot)
            self.enter_orders(instrument={'strike':(self.build['high']+self.curve_flatter*self.underlying_options_step),'call_put':'CE'}, 
                                     buy_sell='sell',quantity = self.tradelot)
            
            self.enter_orders(instrument={'strike':self.build['low'],'call_put':'CE'}, 
                                     buy_sell='buy',quantity = self.tradelot)
            self.enter_orders(instrument={'strike':(self.build['low']-self.curve_flatter*self.underlying_options_step),'call_put':'PE'}, 
                                     buy_sell='sell',quantity = self.tradelot)


            self.build['strategy'] = 'away'
            self.active = stay_active
            self.done_for_the_day = is_done_for_the_day

    def enter_orders(self,instrument, buy_sell, quantity) -> None:
        """Populate Orderbook

        Args:
            instrument ([String]): Name of instrument as in df/NSE
            buy_sell ([Strine]): buy or sell to indicate trde type
            quantity (float): quantity to be traded
            open_close (String): open close. open: trade is opening a new position, close: trade is closing earlier position
        """
        if buy_sell == 'sell':
            quantity *= -1
        order = {'instrument':instrument,'buy_sell':buy_sell,'quantity':quantity}
        logger.info(f"{self.timestamp.date()}|{self.timestamp.time()}| |Order Sent -> {order}", extra=self.class_name_dict_for_logger)
        self.orderbook.append(order)
        self.current_orderbook.append(order)  

    def check_time (self,timestamp):

        logger.info(f"{timestamp.date()}|{timestamp.time()}| ", extra=self.class_name_dict_for_logger)
        self.timestamp = timestamp
        if (timestamp.time() > self.entrytime) & (not self.active) & (not self.done_for_the_day):
            logger.warning(f'{timestamp.date()}|{timestamp.time()}| |Entering market', extra=self.class_name_dict_for_logger)
            self.active = True
        elif (timestamp.time() > self.exittime) & (self.active):
            logger.warning(f'{timestamp.date()}|{timestamp.time()}| |Exiting market', extra=self.class_name_dict_for_logger)
            self.get_away_from_market (stay_active=False)
            self.done_for_the_day = True     

    def analyse_pnl_risk (self, running_pnl, timestamp) -> None:
        """The Analyst analyse current market conditions to access risk.
            If the Running PnL has exceeded Maximum Allowed Daily risk level, analyst will get away from market and cease to be active
    
        Args:
            running_pnl ([float]): Cumulative (Running) Profit or Loss for the day
            timestamp (datetime.time): timestamp for loggin purpose
        """
        logger.info(f"{timestamp.date()}|{timestamp.time()}| | Max PnL risk set to {self.max_rupee_loss}", extra=self.class_name_dict_for_logger)
        self.timestamp = timestamp
        if self.active:
            if running_pnl < self.max_rupee_loss:
                logger.warning(f'{timestamp.date()}|{timestamp.time()}| |pnl={running_pnl}, closing all positions', extra=self.class_name_dict_for_logger)
                self.get_away_from_market (stay_active=False, is_done_for_the_day=True)

    def analyse_movement_risk (self, underlying_ltp, timestamp) -> None:
        """The Analyst analyse current market conditions to access risk.
            If the underlying runs fast and gets out of grasp, analyst will get away from the market but will stay active for next opportunity
    
        Args:
            underlying_ltp (float): Last Traded Price of the Underlying
            timestamp (datetime.time): timestamp for loggin purpose
        """ 
        logger.info(f"{timestamp.date()}|{timestamp.time()}|{underlying_ltp}", extra=self.class_name_dict_for_logger)
        self.timestamp = timestamp
        if self.active:
            if self.build['strategy'] != "away":
                if (self.build['strategy'] == 'strangle'):
                    if (underlying_ltp - self.build['high'] > (self.underlying_options_step * .1)) & \
                        (self.build['low'] - underlying_ltp > (self.underlying_options_step * .1)):
                        logger.warning(f"{timestamp.date()}|{timestamp.time()}|{underlying_ltp}|{self.underlying_name} went out of grasp, going away till {self.underlying_name} in grasp again", extra=self.class_name_dict_for_logger)
                        self.get_away_from_market (stay_active=True)

                elif (self.build['strategy'] == 'straddle'):
                    if abs(self.build['low'] - underlying_ltp) > (self.underlying_options_step * 1.1):
                        logger.warning(f"{timestamp.date()}|{timestamp.time()}|{underlying_ltp}|{self.underlying_name} went out of grasp, going away till {self.underlying_name} in grasp again", extra=self.class_name_dict_for_logger)
                        self.get_away_from_market (stay_active=True)

    def analyse_data (self,underlying_ltp,timestamp, data_guy) -> None:
        """Analyse the Underlying update orderbook by entering trades

        Args:
            underlying_ltp (float): Last Traded Price of Underlying
            timestamp (datetime.time): timestamp for logging
            data_guy (Data_guy): Reference to Data Guy 
        """
        build_string = "        o          "
        if self.build['strategy'] == "straddle":
            build_string = f"""{self.build['low']} ^ {self.build['high']}"""
        elif self.build['strategy'] == "strangle":
            build_string = f"""{self.build['low']} - {self.build['high']}"""
        logger.info(f"{timestamp.date()}|{timestamp.time()}|{underlying_ltp}|{build_string}", extra=self.class_name_dict_for_logger)
        self.timestamp = timestamp
        if self.active:
            self.current_orderbook.clear()
            # self.positions_to_be_closed.clear()
            available_strikes = pd.Series(data_guy.options_strike_range)
            if self.build['strategy'] == "away":
                closeness = available_strikes - underlying_ltp
                if closeness.abs().min() < (self.underlying_options_step * .1):
                    strike = available_strikes[closeness.abs().idxmin()]
                    
                    self.enter_orders(instrument={'strike':(strike-self.curve_flatter*self.underlying_options_step),'call_put':'PE'}, 
                                     buy_sell='buy',quantity = self.tradelot)
                    self.enter_orders(instrument={'strike':strike,'call_put':'CE'}, 
                                     buy_sell='sell',quantity = self.tradelot)
                    
                    self.enter_orders(instrument={'strike':(strike+self.curve_flatter*self.underlying_options_step),'call_put':'CE'}, 
                                     buy_sell='buy',quantity = self.tradelot)
                    self.enter_orders(instrument={'strike':strike,'call_put':'PE'}, 
                                     buy_sell='sell',quantity = self.tradelot)
                    
                    self.build = {'strategy':'straddle','low':strike,'high':strike}
                    logger.warning(f"{timestamp.date()}|{timestamp.time()}|{underlying_ltp}|Straddle entered -> {self.build}", extra=self.class_name_dict_for_logger)
            
            elif self.build['strategy'] == "straddle":
                if underlying_ltp - self.build['high'] > self.underlying_options_step/2:
                    
                    strike = self.build['high'] + self.underlying_options_step

                    
                    self.enter_orders(instrument={'strike':self.build['high'],'call_put':'PE'}, 
                                     buy_sell='buy',quantity = self.tradelot)
                    self.enter_orders(instrument={'strike':(self.build['high']+self.curve_flatter*self.underlying_options_step),'call_put':'CE'}, 
                                     buy_sell='sell',quantity = self.tradelot)
                    
                    self.enter_orders(instrument={'strike':(strike+self.curve_flatter*self.underlying_options_step),'call_put':'CE'}, 
                                     buy_sell='buy',quantity = self.tradelot)
                    self.enter_orders(instrument={'strike':strike,'call_put':'PE'}, 
                                     buy_sell='sell',quantity = self.tradelot)
                    

                    self.build['strategy'] = 'strangle'
                    self.build['high'] = strike
                    logger.warning(f"{timestamp.date()}|{timestamp.time()}|{underlying_ltp}|Drift to Strangle High-> {self.build}", extra=self.class_name_dict_for_logger)
                
                elif underlying_ltp - self.build['low'] < (self.underlying_options_step/2 * -1):

                    strike = self.build['low'] - self.underlying_options_step
                    self.enter_orders(instrument={'strike':self.build['low'],'call_put':'CE'}, 
                                     buy_sell='buy',quantity = self.tradelot)
                    self.enter_orders(instrument={'strike':(self.build['high']-self.curve_flatter*self.underlying_options_step),'call_put':'PE'}, 
                                     buy_sell='sell',quantity = self.tradelot)

                    self.enter_orders(instrument={'strike':(strike-self.curve_flatter*self.underlying_options_step),'call_put':'PE'}, 
                                     buy_sell='buy',quantity = self.tradelot)
                    self.enter_orders(instrument={'strike':strike,'call_put':'CE'}, 
                                     buy_sell='sell',quantity = self.tradelot)
                    

                    self.build['strategy'] = 'strangle'
                    self.build['low'] = strike
                    logger.warning(f"{timestamp.date()}|{timestamp.time()}|{underlying_ltp}|Drift to Strangle Low-> {self.build}", extra=self.class_name_dict_for_logger)

            elif self.build['strategy'] == "strangle":
                if underlying_ltp - self.build['high'] > (self.underlying_options_step * -.1):
                    strike = self.build['high']

                    self.enter_orders(instrument={'strike':self.build['low'],'call_put':'CE'}, 
                                     buy_sell='buy',quantity = self.tradelot)
                    self.enter_orders(instrument={'strike':(self.build['low']-self.curve_flatter*self.underlying_options_step),'call_put':'PE'}, 
                                     buy_sell='sell',quantity = self.tradelot)
                    
                    self.enter_orders(instrument={'strike':(strike-self.curve_flatter*self.underlying_options_step),'call_put':'PE'}, 
                                     buy_sell='buy',quantity = self.tradelot)
                    self.enter_orders(instrument={'strike':strike,'call_put':'CE'}, 
                                     buy_sell='sell',quantity = self.tradelot)

                    self.build['strategy'] = 'straddle'
                    self.build['low'] = strike
                    logger.warning(f"{timestamp.date()}|{timestamp.time()}|{underlying_ltp}|Drift to Straddle High -> {self.build}", extra=self.class_name_dict_for_logger)

                elif underlying_ltp - self.build['low'] < (self.underlying_options_step * .1):
                    strike = self.build['low']

                    self.enter_orders(instrument={'strike':self.build['high'],'call_put':'PE'}, 
                                     buy_sell='buy',quantity = self.tradelot)
                    self.enter_orders(instrument={'strike':(self.build['high']+self.curve_flatter*self.underlying_options_step),'call_put':'CE'}, 
                                     buy_sell='sell',quantity = self.tradelot)

                    self.enter_orders(instrument={'strike':(strike+self.curve_flatter*self.underlying_options_step),'call_put':'CE'}, 
                                     buy_sell='buy',quantity = self.tradelot)
                    self.enter_orders(instrument={'strike':strike,'call_put':'PE'}, 
                                     buy_sell='sell',quantity = self.tradelot)

                    self.build['strategy'] = 'straddle'
                    self.build['high'] = strike
                    logger.warning(f"{timestamp.date()}|{timestamp.time()}|{underlying_ltp}|Drift to Straddle Low -> {self.build}", extra=self.class_name_dict_for_logger)
        

class Trader:

    def __init__ (self, underlying_name, options_df, money_bag=0) -> None:
        self.underlying_name = underlying_name.upper()
        self.positions = pd.DataFrame()
        self.positions_added = pd.DataFrame()
        self.options_df = options_df
        self.money_bag = money_bag
        self.realized_pnl = 0
        self.class_name_dict_for_logger = {'className':self.__class__.__name__}
        print("Trader is Ready")
    
    def start_trading(self,timestamp, current_orderbook, underlying_ltp) -> None:
        """Picks up the current orderbook and starts putting in the orders

        Args:
            timestamp (datetime.time): timestamp
            current_orderbook (list): List of current orders in pipeline
            underlying_ltp (float): Last Traded Price of Underlying
        """
        self.positions_added = self.positions_added.iloc[0:0]
        for _ in range(len(current_orderbook)):
            trade = current_orderbook.pop(0)
            
            next_available_quote = self.get_quote(instrument = trade['instrument'],
                buy_sell=trade['buy_sell'],timestamp=timestamp)
            
            cash_flow = trade['quantity'] * next_available_quote * -1

            self.money_bag += cash_flow

            self.realized_pnl += cash_flow

            position = {'strike':trade['instrument']['strike'],'call_put':trade['instrument']['call_put'],
                        'quantity': trade['quantity']}

            self.positions_added = self.positions_added.append(position, ignore_index=True)
            self.positions = self.positions.append(position, ignore_index=True)
            logger.warning(f"{timestamp.date()}|{timestamp.time()}|{underlying_ltp}|Position Entered - {position}: {next_available_quote}", extra=self.class_name_dict_for_logger)

    
    def get_quote (self,instrument, buy_sell,timestamp) -> float:
        """Returns the next available quote based of the instrument based on buy or sell order

        Args:
            instrument (string): instrument name as in df/NSE
            buy_sell (String): buy or sell order
            timestamp (datetime.time): timestamp

        Returns:
            float: next available quote from df
        """
        price_col = 'SellPrice'
        if buy_sell == 'Sell':
            price_col = 'BuyPrice'
        try:
            price = self.options_df[(self.options_df['timestamp'] > timestamp) & (self.options_df['strike']==instrument['strike']) & 
                (self.options_df['call_put']==instrument['call_put'])][price_col].iloc[0]
        except IndexError:
            price = None
        
        return price
    

class Bookkeeper:
    
    def __init__(self,options_df, cost_per_transaction = 20):
        self.positions = pd.DataFrame()
        self.options_df = options_df
        self.pnl = 0
        self.unrealized_pnl = 0
        self.number_of_transactions = 0
        self.number_of_transactions_to_close = 0
        self.cost_per_transaction = cost_per_transaction
        self.class_name_dict_for_logger = {'className':self.__class__.__name__}
   
        print("Bookkeeper is Ready")
        pass

    def add_new_positions(self,new_positions) -> None:
        """Add newly created positions to positions

        Args:
            new_positions (DataFrame): DataFrame of newly created positions
        """  
        self.number_of_transactions += len(new_positions)
        self.number_of_transactions_to_close = self.number_of_transactions  
        self.positions = self.positions.append(new_positions, ignore_index=True)
        self.positions = self.positions.groupby(['strike','call_put'])['quantity'].sum().reset_index() #Aggregate quantities
        self.positions = self.positions[self.positions['quantity']!=0]

    def track_current_open_positions (self,timestamp, realized_pnl) -> None:
        """Track open positions by fetching the LTP and calculating individual PnL for all positions.
           Also calculates the pnl

        Args:
            timestamp (datetime.datetime): Current Timestamp at which LTP and PnL will be calculated
        """        
        if not self.positions.empty:
            self.positions['last_update'] = timestamp
            self.positions = self.positions[['strike','call_put','quantity','last_update']].merge(self.options_df[['timestamp','strike','call_put','LTP']],how='left',
                                    left_on=['last_update','strike','call_put'],right_on=['timestamp','strike','call_put'])

            self.positions ['PL'] = self.positions['LTP'] * self.positions['quantity']
            self.unrealized_pnl = self.positions['PL'].sum()
            self.number_of_transactions_to_close += len(self.positions)
        else:
            self.unrealized_pnl = 0
        
        self.pnl = self.unrealized_pnl + realized_pnl - (self.number_of_transactions_to_close*self.cost_per_transaction)
        try:
            positions_string = self.positions[['strike','call_put','quantity','LTP','PL']].to_dict(orient='records')
        except KeyError:
            positions_string = " "
        
        logger.info(f"{timestamp.date()}|{timestamp.time()}| |{positions_string}->{realized_pnl}+{self.unrealized_pnl}-{self.number_of_transactions_to_close}*{self.cost_per_transaction}:={self.pnl}", extra=self.class_name_dict_for_logger)

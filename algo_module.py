import pandas as pd
from datetime import datetime, timedelta
from time import perf_counter, sleep, perf_counter_ns
from py_vollib.black_scholes.implied_volatility import implied_volatility as bs_iv
from py_vollib.black_scholes.greeks import analytical as greeks
from broker_module import Broker
import logging
from pandas.core.arrays import boolean
from functools import wraps

# making some changes
logger1 = None

class Logger ():
    """
    Logger is a class designed to log events. 
    It initiates a logger object which can be used to log events across the script,
    as per desired format and values.
    Format: '(levelname)|(className)->(functionName)|(date)|(time)|(other_passed_items:value)'
    Eg call statement log(underlying='NIFTY',ltp=18000), 
        this will log required elements like current_datetime, 
        and passed elements like underlying and ltp
    """    

    def __init__(self, is_logging, data_guy= None, 
            broker_for_trade="0", 
            broker_for_data="0", 
            log_folder="Logs", 
            trading_date=None) -> None:
        """Initialize the logger

        Args:
            is_logging (bool): True if we are logging the execution
            data_guy (Data_guy, optional): data_guy object 
                required to extract current datetime. 
                Defaults to None.
            broker_for_trade (str, optional): broker name 
                used for trading, 
                used to set log filename. 
                Defaults to "0".
            broker_for_data (str, optional): broker name 
                used for data fetching, 
                used to set log filename. 
                Defaults to "0".
            log_folder (str, optional): folder name 
                used to store log files. 
                Defaults to "Logs".
            trading_date (date object, optional): Trade Execution date,
                used to set log filename.
                Defaults to None.
        """    

        # if trading date is not provided, default it to today's date    
        if trading_date is None: trading_date = datetime.now().date()
        self.is_logging = is_logging
        self.logger = None

        # if logging execution, set logging object
        if self.is_logging:
            self.logger = logging.getLogger(__name__)
            self.logger.setLevel(logging.INFO)
            formatter = logging.Formatter('%(levelname)s|%(className)s->%(functionName)s|%(message)s')

            timestamp_string = datetime.now().strftime("%H_%M_%S")
            file_handler = logging.FileHandler(
                f'./{log_folder}/T-{broker_for_trade} D-{broker_for_data} {trading_date} @ {timestamp_string} team.log')
            file_handler.setFormatter(formatter)

            self.logger.addHandler(file_handler)

        self.log_levels = {'CRITICAL':50,
                           'ERROR':40,
                           'WARNING':30,
                           'INFO':20,
                           'DEBUG':10,
                           'NOTSET':0}
        
        self.data_guy = data_guy

    def log (self,extra=None,**kwargs) -> None:
        """function used to create a log entry using (if provided):
                extra['className']: class name of calling function
                extra['functionName']: function name
                level: log level
                kwargs: and additional arguments passed

        Args:
            extra (dict, optional): {'className':__,'functionName':__}.
                Defaults to None.
        """        
        #prepare the log string
        # Format: '(levelname)|(className)->(functionName)|(date)|(time)|(other_passed_items:value)'
        if self.is_logging:
            logger_string = ""
            if extra is None:
                #className and functionName extracted
                extra = {\
                    'className': "",
                    'functionName': ""}
            if 'level' in kwargs:
                #levelname extracted
                numeric_log_level = \
                    self.log_levels[kwargs.pop('level').upper()]
            else: 
                #default level as INFO
                numeric_log_level = \
                    self.log_levels['INFO']

            try:
                #extracting date and time 
                current_datetime_string = f"\
                    {self.data_guy.current_datetime.date()} \
                        | \
                    {self.data_guy.current_datetime.time()} \
                        | "
                current_datetime_string = " ".\
                        join(current_datetime_string.\
                            split())
            except AttributeError: 
                #if not available use blank
                current_datetime_string= " -|-|- "
            
            logger_string = logger_string + \
                    current_datetime_string
            
            #Extract additional elements 
            for key,value in kwargs.items():
                value_modified = value
                if isinstance(value,pd.DataFrame) \
                    or isinstance (value,pd.Series):
                    value_modified = value.to_json()
                key_value_string = f"{key}-{value_modified};"
                logger_string = \
                    logger_string + key_value_string

            #Create log enter
            self.logger.log(level=numeric_log_level,
                msg=f"|{logger_string}",
                extra=extra)

# Decorator to add log for called and result of function 
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
            #Capruting className and functionName
            class_function_name_dict = {\
                    'className': args[0].__class__.__name__,
                    'functionName': original_function.__name__}
            try:
                logger1.log(status="Called",extra=class_function_name_dict,**kwargs_decorator)
            except: 
                #pass if logger is not set
                pass
            
            #Time function execution
            start_time = perf_counter_ns()
            try:
                result = original_function(*args,**kwargs)
                end_time = perf_counter_ns()
                logger1.log(result=result,
                    extra=class_function_name_dict,
                    status="End",execution_time = (end_time-start_time)/1000,
                    **kwargs_decorator)
                return result
            except Exception as e:
                logger1.log(status="Exception",e=e,
                extra=class_function_name_dict,
                execution_time = (end_time-start_time)/1000,
                **kwargs_decorator)
                #return default value in case of error
                return default_return
        return wrapper_function
    return decorator_function

class Data_guy:
    """
    Data_guy is class which will fetch/calculate and store 
    all data related to the strategy.
    Current data values:
        current_datetime
        current_pnl
        strategy_pnl (pnl excluding brokerage)
        brokerage_pnl (brokerage fee)
        max_pnl (max pnl reached during execution)
        trailing_pnl (loss since max pnl)
        current_ltp (ltp of underlying)
        data_df (dataframe storing events for each iteration)
        underlying_name
        expiry_datetime (next expiry)
        candle_length (length of candle used for analysis)
    
    Calculators
        Options Greeks
        ATM Strike
        Candle OHLC
    """    

    def __init__ (self) -> None:
        """Initialize blank data_guy object
        """        
        pass

    
    @keep_log()
    def set_parameters(self, broker, trader, underlying_name,
                 events_and_actions = None,
                 current_datetime=None, \
                 candle_length=None) -> None:
        """Set parameters of Broker Object

        Args:
            broker (Broker): Broker Object
            trader (Trader): Trader Object
            underlying_name (str): Name of underlying eg NIFTY
            events_and_actions (Events_and_action, optional): Events_and_actions object. Defaults to None.
            current_datetime (datetime.datetime, optional): current datetime. Defaults to None.
        """

        #if current datetime is not provided, default it to now()        
        if current_datetime is None: current_datetime = datetime.now()
        self.current_datetime = current_datetime

        #set pnl parameters to 0
        self.current_pnl = 0
        self.strategy_pnl = 0
        self.brokerage_pnl = 0
        self.max_pnl = 0
        self.trailing_pnl = 0

        #set LTP as None
        self.current_ltp = None

        #Initiate data_df and store as csv
        self.data_df = pd.DataFrame()
        timestamp_string = datetime.now().strftime("%Y-%m-%d %H_%M_%S")
        self.data_df_store_path = f'./Data_df/Data {timestamp_string} team.csv'
        self.data_df.to_csv(self.data_df_store_path, index=False)
        
        self.broker = broker
        self.trader = trader
        self.events_and_actions = events_and_actions

        self.underlying_name = underlying_name.upper()
        
        #Get expiry datetime provided by Data Broker
        self.expiry_datetime = self.broker.get_next_expiry_datetime(
            self.underlying_name) 
        self.is_expiry_day = False
        if self.expiry_datetime.date() == self.current_datetime.date(): self.is_expiry_day = True
        
        self.candle_length = candle_length

        self.candle_t_2 = {'start_datetime':None,
                            'end_datetime':None,
                            'open':None,
                            'high':None,
                            'low':None,
                            'close':None}

        self.candle_t_1 = {'start_datetime':None,
                            'end_datetime':None,
                            'open':None,
                            'high':None,
                            'low':None,
                            'close':None}
        
        logger1.log(info="Data Guy Initiated")

    
    @keep_log(default_return=False)
    def update_data(self, current_datetime=None) -> boolean:
        """Update data based on the current_datetime
            Items updated are:
                PnL
                LTP
                Candle Values
                Data DataFrame

        Args:
            current_datetime (datetime.datetime, optional): current_datetime. Defaults to None.

        Returns:
            boolean: True if update was successful
                False if not
        """        

        self.current_datetime = current_datetime

        # Fetch strategy pnl and brokerage/trader fee
        self.strategy_pnl = round(self.broker.get_pnl(),2)
        self.brokerage_pnl = round(self.trader.total_trade_fee,2)
        self.current_pnl = round(self.strategy_pnl  + self.brokerage_pnl,2)

        #Update max pnl if last max pnl breached
        if self.current_pnl > self.max_pnl: self.max_pnl = self.current_pnl
        logger1.log(pnl=self.current_pnl, max_pnl=self.max_pnl)
        self.trailing_pnl = self.current_pnl - self.max_pnl

        #update current ltp
        self.current_ltp = self.broker.get_ltp(self.underlying_name)

        #update candles
        self.candle_t_2 = self.candle(t_minus=2,candle_type='fixed')
        self.candle_t_1 = self.candle(t_minus=1,candle_type='fixed')

        #Update data dataframe
        self.data_df = self.data_df.append({'underlying_name': self.underlying_name, \
                                            'current_datetime': self.current_datetime, \
                                            'current_pnl': self.current_pnl,\
                                            'strategy_pnl':self.strategy_pnl,\
                                            'brokerage_pnl':self.brokerage_pnl,\
                                            'max_pnl': self.max_pnl, \
                                            'trailing_pnl': self.trailing_pnl,
                                            'current_ltp': self.current_ltp, \
                                            'expiry_datetime': self.expiry_datetime, \
                                            'current_position': self.events_and_actions.current_position, \
                                            'straddle_strike': self.events_and_actions.straddle_strike, \
                                            'strangle_strike_low': self.events_and_actions.strangle_strike_low, \
                                            'strangle_strike_low': self.events_and_actions.strangle_strike_high, \
                                            'position_entry_ltp': self.events_and_actions.position_entry_ltp, \
                                            'is_hedged': self.events_and_actions.is_hedged, \
                                            'is_closed': self.events_and_actions.is_closed,\
                                            'candle_t_2_start':self.candle_t_2['start_datetime'],\
                                            'candle_t_2_end':self.candle_t_2['end_datetime'],\
                                            'candle_t_2_open':self.candle_t_2['open'],\
                                            'candle_t_2_close':self.candle_t_2['close'],\
                                            'candle_t_1_start':self.candle_t_1['start_datetime'],\
                                            'candle_t_1_end':self.candle_t_1['end_datetime'],\
                                            'candle_t_1_open':self.candle_t_1['open'],
                                            'candle_t_1_close':self.candle_t_1['close']}, \
                                            ignore_index=True)
        #save data_df
        self.data_df.to_csv(self.data_df_store_path, index=False)

        update_successful = True
        if update_successful:
            logger1.log(current_ltp=self.current_ltp)

        return update_successful

    @keep_log()
    def get_atm_strike(self) -> int:
        """Get ATM strike value based on underlying LTP

        Returns:
            int: ATM Strike based on underlying LTP
        """        
        atm_strike = 50 * round(self.current_ltp / 50)
        return atm_strike

    @keep_log(default_return = {'start_datetime':None,
                            'end_datetime':None,
                            'open':None,
                            'high':None,
                            'low':None,
                            'close':None})
    def candle(self,t_minus=1, candle_type = 'dynamic') -> float:
        """Returns OHLC Candle.
            Candle type can be of two types:
            1) dynamic: Candle length is from current time,
                        For Eg is the candle length is 5 minutes and time is 9:28AM
                        t_minus 1 will refer to candle 9:23 - 9:28
                        t_minus 2 will refer to candle 9:18 - 9:23

            2) fixed:   Fixed candle length is used,
                        For Eg is the candle length is 5 minutes and time is 9:28AM
                        t_minus 1 will refer to candle 9:20 - 9:25
                        t_minus 2 will refer to candle 9:15 - 9:20

        Args:
            t_minus (int, optional): t_minus historical candle. Defaults to 1.
            candle_type (str, optional): dynamic/fixed. Defaults to 'dynamic'.

        Returns:
            dict: candle_dict - 
                {'start_datetime': datetime, 
                'end_datetime': datetime,
                'open': float,
                'high': float,
                'low': float,
                'close': float}
        """        

        if len(self.data_df) == 0:
            return {'start_datetime':None,
                            'end_datetime':None,
                            'open':None,
                            'high':None,
                            'low':None,
                            'close':None}

        end_datetime = self.current_datetime + \
            timedelta(minutes=(\
                self.candle_length*(t_minus-1)*-1)\
                )

        #calculate fixed length end time
        if candle_type == 'fixed':
            end_datetime = end_datetime - \
            (end_datetime - datetime.min) \
                % timedelta(minutes=\
                    self.candle_length)

        start_datetime = end_datetime - \
            timedelta(minutes=self.candle_length)

        #slice data_df for required candle period
        candle_df = self.data_df[(self.data_df['current_datetime'] >= start_datetime) \
                                    & (self.data_df['current_datetime'] <= end_datetime)] \
                                ['current_ltp']
        
        candle_close = None
        candle_open = None
        candle_high = None
        candle_low = None

        if len(candle_df) >= 2:
            candle_close = candle_df.iloc[-1]
            candle_open = candle_df.iloc[0]
            candle_high = candle_df.max()
            candle_low = candle_df.min()

        candle_dict = {'start_datetime':start_datetime,
            'end_datetime':end_datetime,
            'open':candle_open,
            'high':candle_high,
            'low':candle_low,
            'close':candle_close}

        return candle_dict


    @keep_log()
    def calculate_greeks(self, df, greek_type='delta', risk_free_rate=.07, inplace=True, filter_iv=1) -> pd.DataFrame:
        """Calculate greeks for options passed in as a DataFrame

        Args:
            df (dataframe): DataFrame of options. The df should have columns:

            greek_type (str, optional): delta/gamma/rho/theta/vega. Defaults to 'delta'.
            risk_free_rate (float, optional): risk free rate to consider. Defaults to .07.
            inplace (bool, optional): calculate values in same df passed. Defaults to True.
            filter_iv (int, optional): remove options where IV is greater than filter IV. Defaults to 1.

        Returns:
            pd.DataFrame: DataFrame where greeks are calculated and stored in another column
        """        
        ## df should have expiry_datetime, call_put, instrument_ltp and strike columns

        logger1.log(greek_type=greek_type,
            risk_free_rate=risk_free_rate,
            df=df.to_json())


        if not inplace:
            df = df.copy()

        df['time_to_expiry_years'] = (self.expiry_datetime \
                                      - self.current_datetime).total_seconds() \
                                     / timedelta(days=364).total_seconds()

        call_put_pyvollib_map = {'CE': 'c', 'PE': 'p'}
        df['call_put_pyvollib'] = df['call_put'].map(call_put_pyvollib_map)

        #function to calculate IV for each row
        def calculate_iv(each_row):
            iv_value = 0
            try:
                iv_value = bs_iv(price=each_row['instrument_ltp'], S=self.current_ltp,
                                 K=each_row['strike'], t=each_row['time_to_expiry_years'],
                                 r=risk_free_rate, flag=each_row['call_put_pyvollib'])

            except Exception as e:
                iv_value = None
            return iv_value

        #function to calculate desired greek for each row
        def calculate_greek(each_row):
            greek_value = 0
            try:
                if greek_type == 'delta':
                    greek_value = greeks.delta(flag=each_row['call_put_pyvollib'],
                                               S=self.current_ltp, K=each_row['strike'],
                                               t=each_row['time_to_expiry_years'], r=risk_free_rate,
                                               sigma=each_row['implied_volatility'])
                elif greek_type == 'gamma':
                    greek_value = greeks.gamma(flag=each_row['call_put_pyvollib'],
                                               S=self.current_ltp, K=each_row['strike'],
                                               t=each_row['time_to_expiry_years'], r=risk_free_rate,
                                               sigma=each_row['implied_volatility'])
                elif greek_type == 'rho':
                    greek_value = greeks.rho(flag=each_row['call_put_pyvollib'],
                                             S=self.current_ltp, K=each_row['strike'],
                                             t=each_row['time_to_expiry_years'], r=risk_free_rate,
                                             sigma=each_row['implied_volatility'])
                elif greek_type == 'theta':
                    greek_value = greeks.theta(flag=each_row['call_put_pyvollib'],
                                               S=self.current_ltp, K=each_row['strike'],
                                               t=each_row['time_to_expiry_years'], r=risk_free_rate,
                                               sigma=each_row['implied_volatility'])
                elif greek_type == 'vega':
                    greek_value = greeks.vega(flag=each_row['call_put_pyvollib'],
                                              S=self.current_ltp, K=each_row['strike'],
                                              t=each_row['time_to_expiry_years'], r=risk_free_rate,
                                              sigma=each_row['implied_volatility'])
            except Exception as e:
                greek_value = None
            return greek_value

        #calculate IV first and filter based on filter iv value
        df['implied_volatility'] = df.apply(calculate_iv, axis=1)
        df = df[df['implied_volatility'] < filter_iv]

        #calculate desired greek value
        df[greek_type] = df.apply(calculate_greek, axis=1)
        df.drop(columns=['call_put_pyvollib', 'time_to_expiry_years'], inplace=True)

        return df


class Events_and_actions:
    """
    Events_and_actions is the core strategy
    The whole strategy is designed across multiple events
    and all events are mapped to an action.
    Events are designed to have multiple conditions,
    if all the conditions arelated to event are satisfied 
    corresponding action is triggered.
    For eg if event_total_loss_reached is satisfied then 
    action_close_the_day will be triggered. 

    If one event is satisfied during a iteration
    no other event will be checked, thus
    the events should be prioritized.

    Current Events listed based on priority:
        1) event_total_loss_reached() -> action_close_the_day()
        2) event_non_expiry_day_trailing_loss_reached() -> action_close_the_day()
        3) event_expiry_day_trailing_loss_reached() -> action_close_the_day()
        4) event_shop_close_time() -> action_close_the_day()
        5) event_exit_straddle() -> action_exit_position()
        6) event_exit_strangle() -> action_exit_position()
        7) event_expiry_day_enter_position_no_candle_call_first -> action_enter_position_call_first()
        8) event_expiry_day_enter_position_no_candle_put_first -> action_enter_position_put_first()
        9) event_non_expiry_day_enter_position_no_candle_call_first() -> action_enter_position_call_first()
        10) event_non_expiry_day_enter_position_no_candle_put_first() -> action_enter_position_put_first()
        11) event_enter_position_call_first() -> action_enter_position_call_first()
        12) event_enter_position_put_first() -> action_enter_position_put_first()
        13) event_expiry_day_open_shop() -> action_expiry_day_buy_hedge()
        14) event_non_expiry_day_open_shop() -> action_non_expiry_day_buy_hedge()

    List of all actions:
        - action_close_the_day()
        - action_exit_position()
        - action_enter_position_call_first()
        - action_enter_position_put_first()
        - action_expiry_day_buy_hedge()
        - action_non_expiry_day_buy_hedge()

    """    

    
    def __init__(self) -> None:
        """
        Initialize blank events_and_actions object
        """        
        pass
    
    
    @keep_log()
    def set_parameters(self, data_guy, trader, \
                 begin_time=datetime(2020, 1, 1, 9, 28).time(), \
                 close_time=datetime(2020, 1, 1, 15, 7).time(), \
                 total_loss_limit=-1_000, trade_quantity=50, \
                 trailing_loss_trigger=1_500, \
                 max_trailing_loss_non_expiry=-250,
                 max_trailing_loss_expiry=-200,
                 non_expiry_day_no_candle_time = datetime(2020, 1, 1, 14, 30).time(),
                 expiry_day_no_candle_time = datetime(2020, 1, 1, 13, 0).time()) -> None:
        """Set parameters for the 
            events_and_actions object

            events_and_actions have following variables 
            to define state of the strategy:
                is_hedged: True is hedge call and put are bought
                is_closed: if we have closed the day,
                    no trading will be done 
                    after closing the day
                current_position: Straddle/Strangle
                straddle_strike: Strike of Straddle
                strangle_strike_high
                strangle_strike_low
                begin_time: Time when we begin trading
                close_time: Time when we close the day
                position_entry_ltp: LTP at time of 
                    entering the position
                total_loss_limit: Loss at which 
                    we close the day
                trade_quantity: Quantity of instruments traded 
                    in each leg
                trailing_loss_trigger: Profit level at which 
                    we want to book profit 
                    and thus will exit once 
                    we hit a defined trailing loss
                max_trailing_loss_non_expiry: trailing loss 
                    at which we close the day 
                    on non expiry day
                max_trailing_loss_expiry: trailing loss 
                    at which we close the day 
                    on non expiry day
                non_expiry_day_no_candle_time : Time after which 
                    we stop consulting candles 
                    before entering positions 
                    on non expiry day. 
                expiry_day_no_candle_time: Time after which 
                    we stop consulting candles 
                    before entering positions 
                    on expiry day. 
                
                orderbook = pd.DataFrame()
                data_guy = data_guy
                trader = trader

        Args:
            data_guy (Data_guy): data_guy object
            trader (Trader): trader object
            trade_quantity (int, optional): Quantity of instruments 
                traded for each leg. Defaults to 50.
            max_trailing_loss_expiry (int, optional): Max trailing loss 
                before we close the day. Defaults to -200.
            non_expiry_day_no_candle_time (datetime.time, optional): 
                Time after which we stop consulting candles 
                before entering positions 
                on non expiry day. 
                Defaults to datetime(2020, 1, 1, 14, 30).time().
            expiry_day_no_candle_time (datetime.time, optional): 
                Time after which we stop consulting candles 
                before entering positions 
                on expiry day. 
                Defaults to datetime(2020, 1, 1, 13, 0).time().
        """                
        self.is_hedged = False
        self.current_position = None
        self.straddle_strike = None
        self.strangle_strike_high = None
        self.strangle_strike_low = None
        self.is_closed = False
        self.begin_time = begin_time
        self.close_time = close_time
        self.position_entry_ltp = None
        self.total_loss_limit = total_loss_limit
        self.trade_quantity = trade_quantity
        self.trailing_loss_trigger = trailing_loss_trigger
        self.max_trailing_loss_non_expiry = max_trailing_loss_non_expiry
        self.non_expiry_day_no_candle_time = non_expiry_day_no_candle_time
        self.expiry_day_no_candle_time = expiry_day_no_candle_time
        self.max_trailing_loss_expiry = max_trailing_loss_expiry
        self.orderbook = pd.DataFrame()
        self.data_guy = data_guy
        self.trader = trader


    @keep_log()
    def display_string(self):
        """Generate String to represent 
            LTP and current position on the output screen

        Returns:
            str: string to represent LTP and current position
        """        
        display_string = f"{self.data_guy.current_ltp}\
            {self.data_guy.current_pnl}\
            "

        if self.current_position is None:
            display_string += f"-\
                    o\
                    -                                "
        elif self.current_position == 'straddle':
            display_string += f"{self.straddle_strike}\
                    STRADDLE\
                    {self.straddle_strike}\
                        "
        elif self.current_position == 'strangle':
            display_string += f"{self.strangle_strike_low}\
                    STRANGLE\
                    {self.strangle_strike_high}\
                        "

        return display_string

    @keep_log()
    def events_to_actions(self):
        """
        Check all events based on the priority
        if event conditions are satisfied
        mapped action will be called

        Current mapping listed based on priority:
        1) event_total_loss_reached() -> action_close_the_day()
        2) event_non_expiry_day_trailing_loss_reached() -> action_close_the_day()
        3) event_expiry_day_trailing_loss_reached() -> action_close_the_day()
        4) event_shop_close_time() -> action_close_the_day()
        5) event_exit_straddle() -> action_exit_position()
        6) event_exit_strangle() -> action_exit_position()
        7) event_expiry_day_enter_position_no_candle_call_first -> action_enter_position_call_first()
        8) event_expiry_day_enter_position_no_candle_put_first -> action_enter_position_put_first()
        9) event_non_expiry_day_enter_position_no_candle_call_first() -> action_enter_position_call_first()
        10) event_non_expiry_day_enter_position_no_candle_put_first() -> action_enter_position_put_first()
        11) event_enter_position_call_first() -> action_enter_position_call_first()
        12) event_enter_position_put_first() -> action_enter_position_put_first()
        13) event_expiry_day_open_shop() -> action_expiry_day_buy_hedge()
        14) event_non_expiry_day_open_shop() -> action_non_expiry_day_buy_hedge()
        """        

        logger1.log(at='start',
            is_closed=self.is_closed,
            is_hedged=self.is_hedged,
            current_position = self.current_position,
            straddle_strike=self.straddle_strike,
            strangle_strike_high = self.strangle_strike_high,
            strangle_strike_low = self.strangle_strike_low)

        if self.event_total_loss_reached():
            self.action_close_the_day()
        elif self.event_non_expiry_day_trailing_loss_reached():
            self.action_close_the_day()
        elif self.event_expiry_day_trailing_loss_reached():
            self.action_close_the_day()
        elif self.event_shop_close_time():
            self.action_close_the_day()
        elif self.event_exit_straddle():
            self.action_exit_position()
        elif self.event_exit_strangle():
            self.action_exit_position()
        elif self.event_expiry_day_enter_position_no_candle_call_first():
            self.action_enter_position_call_first()
        elif self.event_expiry_day_enter_position_no_candle_put_first():
            self.action_enter_position_put_first()
        elif self.event_non_expiry_day_enter_position_no_candle_call_first():
            self.action_enter_position_call_first()
        elif self.event_non_expiry_day_enter_position_no_candle_put_first():
            self.action_enter_position_put_first()
        elif self.event_enter_position_call_first():
            self.action_enter_position_call_first()
        elif self.event_enter_position_put_first():
            self.action_enter_position_put_first()
        elif self.event_expiry_day_open_shop():
            self.action_expiry_day_buy_hedge()
        elif self.event_non_expiry_day_open_shop():
            self.action_non_expiry_day_buy_hedge()

        logger1.log(at='end',
            is_closed=self.is_closed,
            is_hedged=self.is_hedged,
            current_position = self.current_position,
            straddle_strike=self.straddle_strike,
            strangle_strike_high = self.strangle_strike_high,
            strangle_strike_low = self.strangle_strike_low)



    @keep_log()
    def event_expiry_day_open_shop(self) -> boolean:
        output = False
        if (self.data_guy.is_expiry_day) & (not self.is_closed) & (not self.is_hedged):
            if self.data_guy.current_datetime.time() >= self.begin_time:
                output = True

        return output

    @keep_log()
    def event_non_expiry_day_open_shop(self) -> boolean:

        output = False
        if (not self.data_guy.is_expiry_day) & (not self.is_closed) & (not self.is_hedged):
            if self.data_guy.current_datetime.time() >= self.begin_time:
                output = True

        return output

    @keep_log()
    def event_enter_position_call_first(self) -> boolean:
        output = False
        if (not self.is_closed) & self.is_hedged & (self.current_position == None):
            if self.data_guy.candle_t_2['open'] is not None and self.data_guy.candle_t_2['close'] is not None:
                if abs(self.data_guy.candle_t_2['open'] - self.data_guy.candle_t_2['close']) < 15:
                    if self.data_guy.candle_t_1['open'] is not None and self.data_guy.candle_t_1['close'] is not None:
                        if abs(self.data_guy.candle_t_1['open'] - self.data_guy.candle_t_1['close']) < 15:
                            if abs(self.data_guy.candle_t_1['close']-self.data_guy.current_ltp) < 5:
                                if self.data_guy.current_ltp >= self.data_guy.get_atm_strike():
                                    output = True

        return output

    @keep_log()
    def event_enter_position_put_first(self) -> boolean:

        output = False

        if (not self.is_closed) & self.is_hedged & (self.current_position == None):
            if self.data_guy.candle_t_2['open'] is not None and self.data_guy.candle_t_2['close'] is not None:
                if abs(self.data_guy.candle_t_2['open'] - self.data_guy.candle_t_2['close']) < 15:
                    if self.data_guy.candle_t_1['open'] is not None and self.data_guy.candle_t_1['close'] is not None:
                        if abs(self.data_guy.candle_t_1['open'] - self.data_guy.candle_t_1['close']) < 15:
                            if abs(self.data_guy.candle_t_1['close']-self.data_guy.current_ltp) < 5:
                                if self.data_guy.current_ltp < self.data_guy.get_atm_strike():
                                    output = True
        return output

    @keep_log()
    def event_exit_straddle(self) -> boolean:

        output = False
        if self.current_position == 'straddle':
            if abs(self.data_guy.current_ltp - self.position_entry_ltp) > 10:
                ltp_minus_strike = self.data_guy.current_ltp - self.straddle_strike
                if ltp_minus_strike < -27 or ltp_minus_strike > 27:
                    output = True
        return output


    @keep_log()
    def event_exit_strangle(self) -> boolean:
        output = False
        if self.current_position == 'strangle':
            if abs(self.data_guy.current_ltp - self.position_entry_ltp) > 10:
                ltp_minus_high_strike = self.data_guy.current_ltp - self.strangle_strike_high
                ltp_minus_low_strike = self.data_guy.current_ltp - self.strangle_strike_low
                if ltp_minus_high_strike < -52 or ltp_minus_low_strike > 52:
                    output = True
        return output


    @keep_log()
    def event_expiry_day_enter_position_no_candle_call_first(self) -> boolean:
        output = False
        if (not self.is_closed) & self.is_hedged & (self.current_position == None):
            if (self.data_guy.is_expiry_day) & (
                    self.data_guy.current_datetime.time() >= self.expiry_day_no_candle_time):
                if self.data_guy.current_ltp >= self.data_guy.get_atm_strike():
                    output = True
        return output


    @keep_log()
    def event_expiry_day_enter_position_no_candle_put_first(self) -> boolean:

        output = False
        if (not self.is_closed) & self.is_hedged & (self.current_position == None):
            if (self.data_guy.is_expiry_day) & (
                    self.data_guy.current_datetime.time() >= self.expiry_day_no_candle_time):
                if self.data_guy.current_ltp < self.data_guy.get_atm_strike():
                    output = True

        return output


    @keep_log()
    def event_non_expiry_day_enter_position_no_candle_call_first(self) -> boolean:

        output = False
        if (not self.is_closed) & self.is_hedged & (self.current_position == None):
            if (not self.data_guy.is_expiry_day) & (
                    self.data_guy.current_datetime.time() >= self.non_expiry_day_no_candle_time):
                if self.data_guy.current_ltp >= self.data_guy.get_atm_strike():
                    output = True

        return output


    @keep_log()
    def event_non_expiry_day_enter_position_no_candle_put_first(self) -> boolean:

        output = False

        if (not self.is_closed) & self.is_hedged & (self.current_position == None):
            if (not self.data_guy.is_expiry_day) & (
                    self.data_guy.current_datetime.time() >= self.non_expiry_day_no_candle_time):
                if self.data_guy.current_ltp < self.data_guy.get_atm_strike():
                    output = True
        return output


    @keep_log()
    def event_total_loss_reached(self) -> boolean:
        output = False
        if not self.is_closed:
            if self.data_guy.current_pnl < self.total_loss_limit:
                output = True
        return output


    def event_shop_close_time(self) -> boolean:

        output = False
        if not self.is_closed:
            if self.data_guy.current_datetime.time() >= self.close_time:
                output = True
        return output


    @keep_log()
    def event_non_expiry_day_trailing_loss_reached(self) -> boolean:

        output = False
        if not self.is_closed and not self.data_guy.is_expiry_day:
            if (self.data_guy.max_pnl >= self.trailing_loss_trigger) & (
                    self.data_guy.trailing_pnl <= self.max_trailing_loss_non_expiry):
                output = True

        return output


    @keep_log()
    def event_expiry_day_trailing_loss_reached(self) -> boolean:

            output = False
            if not self.is_closed & self.data_guy.is_expiry_day:
                if (self.data_guy.current_datetime.time() >= datetime(2020, 1, 1, 14, 50).time()):
                    if (self.data_guy.max_pnl >= 1_000) & \
                        (self.data_guy.trailing_pnl <= self.max_trailing_loss_expiry):
                        output = True
            return output


    @keep_log(level='warning')
    def action_expiry_day_buy_hedge(self) -> boolean:
        """
        Buy hedges
        Expiry Day: Buy ₹ 3 Call and ₹ 3 Put
        Non Expiry Day: Buy ₹ 5 Call and ₹ 5 Put

        Returns:
            boolean: True if corresponding trade is successful 
        """        

        output = False

        #identify strike of ₹ 3 Call
        strike, _ = self.trader.strike_discovery(underlying=self.data_guy.underlying_name,
                                                    call_put='CE', expiry_datetime=self.data_guy.expiry_datetime,
                                                    based_on_value='price', value=3)
        #Add the discovered option in orderbook
        order = {'order_id': str(datetime.now()), \
                    'strike': strike, \
                    'underlying': self.data_guy.underlying_name, \
                    'call_put': 'CE', \
                    'expiry_datetime': self.data_guy.expiry_datetime, \
                    'quantity': self.trade_quantity, \
                    'buy_sell': 'buy'}
        self.orderbook = self.orderbook.append(order, ignore_index=True)

        #identify strike of ₹ 3 Put
        strike, _ = self.trader.strike_discovery(underlying=self.data_guy.underlying_name,
                                                    call_put='PE', expiry_datetime=self.data_guy.expiry_datetime,
                                                    based_on_value='price', value=3)
        #Add the discovered option in orderbook
        order = {'order_id': str(datetime.now()), \
                    'strike': strike, \
                    'underlying': self.data_guy.underlying_name, \
                    'call_put': 'PE', \
                    'expiry_datetime': self.data_guy.expiry_datetime, \
                    'quantity': self.trade_quantity,
                    'buy_sell': 'buy'}
        self.orderbook = self.orderbook.append(order, ignore_index=True)

        #Call trader to execute orders in the orderbook
        orders_executed = self.trader.place_order_in_orderbook()

        if orders_executed:
            #Update Variable to indicate change in state
            self.is_hedged = True
            output = True

        return output


    @keep_log(level='warning')
    def action_non_expiry_day_buy_hedge(self) -> boolean:
        """
        Buy hedges
        Expiry Day: Buy ₹ 3 Call and ₹ 3 Put
        Non Expiry Day: Buy ₹ 5 Call and ₹ 5 Put

        Returns:
            boolean: True if corresponding trade is successful 
        """    

        output = False
        #identify strike of ₹ 5 Call
        strike, _ = self.trader.strike_discovery(underlying=self.data_guy.underlying_name,
                                                    call_put='CE', expiry_datetime=self.data_guy.expiry_datetime,
                                                    based_on_value='price', value=5)
        #Add the discovered option in orderbook
        order = {'order_id': str(datetime.now()), 'strike': strike, 'underlying': self.data_guy.underlying_name,
                    'call_put': 'CE', 'expiry_datetime': self.data_guy.expiry_datetime,
                    'quantity': self.trade_quantity,
                    'buy_sell': 'buy'}
        self.orderbook = self.orderbook.append(order, ignore_index=True)

        #identify strike of ₹ 5 Put
        strike, _ = self.trader.strike_discovery(underlying=self.data_guy.underlying_name,
                                                    call_put='PE', expiry_datetime=self.data_guy.expiry_datetime,
                                                    based_on_value='price', value=5)
        #Add the discovered option in orderbook
        order = {'order_id': str(datetime.now()), 'strike': strike, 'underlying': self.data_guy.underlying_name,
                    'call_put': 'PE', 'expiry_datetime': self.data_guy.expiry_datetime,
                    'quantity': self.trade_quantity,
                    'buy_sell': 'buy'}
        self.orderbook = self.orderbook.append(order, ignore_index=True)

        #Call trader to execute orders in the orderbook
        orders_executed = self.trader.place_order_in_orderbook()

        if orders_executed:
            #Update Variable to indicate change in state
            self.is_hedged = True
            output = True
        
        return output

    @keep_log(level='warning')
    def action_enter_position_put_first(self) -> boolean:
        """
        Enter Straddle/Strangle position
        Step 1) Sell Put with delta close to .5
        Step 2) Sell Call with price closest to Put

        Returns:
            boolean: True if all corresponding trades are successful
        """        

        output = False
        #Identify Put with delta close to .5
        strike_put, instrument_ltp = self.trader.strike_discovery(underlying=self.data_guy.underlying_name,
                                                                    call_put='PE',
                                                                    expiry_datetime=self.data_guy.expiry_datetime,
                                                                    based_on_value='delta', value=-.5)
        #Add the discovered option in orderbook
        order = {'order_id': str(datetime.now()), 'strike': strike_put, 'underlying': self.data_guy.underlying_name,
                    'call_put': 'PE', 'expiry_datetime': self.data_guy.expiry_datetime,
                    'quantity': self.trade_quantity,
                    'buy_sell': 'sell'}
        self.orderbook = self.orderbook.append(order, ignore_index=True)

        #Identify Call with price close to Put
        strike_call, _ = self.trader.strike_discovery(underlying=self.data_guy.underlying_name,
                                                        call_put='CE', expiry_datetime=self.data_guy.expiry_datetime,
                                                        based_on_value='price', value=instrument_ltp)
        #Add the discovered option in orderbook
        order = {'order_id': str(datetime.now()), 'strike': strike_call,
                    'underlying': self.data_guy.underlying_name,
                    'call_put': 'CE', 'expiry_datetime': self.data_guy.expiry_datetime,
                    'quantity': self.trade_quantity,
                    'buy_sell': 'sell'}
        self.orderbook = self.orderbook.append(order, ignore_index=True)

        #Call trader to execute orders in the orderbook
        orders_executed = self.trader.place_order_in_orderbook()

        if orders_executed:
            #Update Variable to indicate change in state
            self.position_entry_ltp = self.data_guy.current_ltp
            if strike_put == strike_call:
                self.current_position = 'straddle'
                self.straddle_strike = strike_put
            else:
                self.current_position = 'strangle'
                if strike_put >= strike_call:
                    self.strangle_strike_high = strike_put
                    self.strangle_strike_low = strike_call
                else:
                    self.strangle_strike_high = strike_call
                    self.strangle_strike_low = strike_put
            output = True
        return output

    @keep_log(level='warning')
    def action_enter_position_call_first(self) -> boolean:
        """
        Enter Straddle/Strangle position
        Step 1) Sell Put with delta close to .5
        Step 2) Sell Call with price closest to Put

        Returns:
            boolean: True if all corresponding trades are successful
        """  
        output = False
        #Identify Call with delta close to .5
        strike_call, instrument_ltp = self.trader.strike_discovery( \
            underlying=self.data_guy.underlying_name,
            call_put='CE', expiry_datetime=self.data_guy.expiry_datetime,
            based_on_value='delta', value=.5)
        
        #Add the discovered option in orderbook
        order = {'order_id': str(datetime.now()),
                    'strike': strike_call,
                    'underlying': self.data_guy.underlying_name,
                    'call_put': 'CE',
                    'expiry_datetime': self.data_guy.expiry_datetime,
                    'quantity': self.trade_quantity,
                    'buy_sell': 'sell'}
        self.orderbook = self.orderbook.append(order, ignore_index=True)

        #Identify Put with price close to Call
        strike_put, _ = self.trader.strike_discovery( \
            underlying=self.data_guy.underlying_name,
            call_put='PE', expiry_datetime=self.data_guy.expiry_datetime,
            based_on_value='price', value=instrument_ltp)
        
        #Add the discovered option in orderbook
        order = {'order_id': str(datetime.now()),
                    'strike': strike_put,
                    'underlying': self.data_guy.underlying_name,
                    'call_put': 'PE',
                    'expiry_datetime': self.data_guy.expiry_datetime,
                    'quantity': self.trade_quantity,
                    'buy_sell': 'sell'}
        self.orderbook = self.orderbook.append(order, ignore_index=True)

        #Call trader to execute orders in the orderbook
        orders_executed = self.trader.place_order_in_orderbook()

        if orders_executed:
            #Update Variable to indicate change in state
            self.position_entry_ltp = self.data_guy.current_ltp
            if strike_put == strike_call:
                self.current_position = 'straddle'
                self.straddle_strike = strike_put
            else:
                self.current_position = 'strangle'
                if strike_put >= strike_call:
                    self.strangle_strike_high = strike_put
                    self.strangle_strike_low = strike_call
                else:
                    self.strangle_strike_high = strike_call
                    self.strangle_strike_low = strike_put
            output = True

            return output


    @keep_log(level='warning')
    def action_exit_position(self) -> boolean:
        """Exit current position

        Returns:
            boolean: True if all corresponding trades are successful
        """        

        output = False
        current_position = self.trader.get_positions()

        #Exit only sell positions thus filter out all quantities bought
        current_position = current_position[ \
            current_position['quantity'] < 0]

        #Loop through current positions and add each to orderbook
        for _, each_leg in current_position.iterrows():
            instrument_id = str(each_leg['instrument_id_trade'])
            quantity = abs(each_leg['quantity'])
            exchange = each_leg['exchange']
            if each_leg['quantity'] < 0:
                buy_sell = 'buy'
            elif each_leg['quantity'] > 0:
                buy_sell = 'sell'

            order = {'order_id': str(datetime.now()),
                        'instrument_id_trade': instrument_id,
                        'quantity': quantity,
                        'buy_sell': buy_sell,
                        'exchange': exchange}

            self.orderbook = self.orderbook.append(order, ignore_index=True)
        
        #Call trader to execute orders in the orderbook
        orders_executed = self.trader.place_order_in_orderbook(instrument_id_available=True)

        if orders_executed:
            #Update Variable to indicate change in state
            self.current_position = None
            self.straddle_strike = None
            self.strangle_strike_high = None
            self.strangle_strike_low = None
            self.position_entry_ltp = None
            output = True

        return output

    @keep_log(level='warning')
    def action_close_the_day(self) -> boolean:
        """Close the day and square off all positions

        Returns:
            boolean: True if all corresponding trades are successful
        """        

        output = False
        #Get all current positions
        current_position = self.trader.get_positions()

        
        if len(current_position) != 0:

            #Filter out already closed positions
            current_position = current_position[ \
                current_position['quantity'] != 0]\
                    .sort_values(by='quantity')
                
            #Loop through current positions and add each to orderbook
            for _, each_leg in current_position.iterrows():
                instrument_id = str(each_leg['instrument_id_trade'])
                quantity = abs(each_leg['quantity'])
                exchange = each_leg['exchange']
                if each_leg['quantity'] < 0:
                    buy_sell = 'buy'
                elif each_leg['quantity'] > 0:
                    buy_sell = 'sell'

                order = {'order_id': str(datetime.now()),
                            'instrument_id_trade': instrument_id,
                            'quantity': quantity,
                            'buy_sell': buy_sell,
                            'exchange': exchange}

                self.orderbook = self.orderbook.append(order, ignore_index=True)

        #Call trader to execute orders in the orderbook
        orders_executed = self.trader.place_order_in_orderbook(instrument_id_available=True)

        if orders_executed:
            #Update Variable to indicate change in state
            self.current_position = None
            self.straddle_strike = None
            self.strangle_strike_high = None
            self.strangle_strike_low = None
            self.position_entry_ltp = None
            self.is_hedged = False
            self.is_closed = True
            output = True

        return output


class Trader:
    """
    Trader is responsible to communicate with broker and carry out trades
    Current Capabilities:
        Place_orders_in_orderbook
        Strike Discovery based on Price or delta
        Get Positions
    """    
    
    def __init__(self) -> None:
        """
        Initialize blank trader object
        """        
        pass
    
    
    @keep_log()
    def set_parameters(self, broker, data_guy, events_and_actions,per_trade_fee=0) -> None:
        """
        Set parameters to the traderobject

        Args:
            broker (Broker): Broker object
            data_guy (Data_guy): Data_guy object
            events_and_actions (Events_and_action): Events_and_actions object
            per_trade_fee (int, optional): Brokerage fee per trade. Defaults to 0.
        """        

        self.broker = broker
        self.data_guy = data_guy
        self.events_and_actions = events_and_actions
        self.tradebook = pd.DataFrame()
        self.current_positionbook = pd.DataFrame()
        self.per_trade_fee = per_trade_fee
        self.total_trade_fee = 0


    @keep_log(default_return=False)
    def place_order_in_orderbook(self, wait_time_secs=3, instrument_id_available=False) -> boolean:
        """Iters through the orderbook and places all orders
            Two ways orderbook can be used
            1) instrument_ID is available:
                instrument_id_trade
                buy_sell
                quantity
                exchange

                Set instrument_id_available param to True

            2) instrument_id is not availbale:
                Strike
                call_put
                expiry_datetime
                buy_sell
                quantity
                exchange

                Set instrument_id_available as False in case 2

            In Case 2 trader first calculates instrument_id based on:
                Strike
                call_put
                expiry_datetime
            and then place order

        Args:
            wait_time_secs (int, optional): Time to wait in seconds 
                                for completion of each order
                                If not completed move to next. 
                                Defaults to 3.
            instrument_id_available (bool, optional): Orders can be placed . Defaults to False.

        Returns:
            boolean: [description]
        """        

        logger1.log(orderbook=self.events_and_actions.orderbook.to_json(),
            instrument_id_available=instrument_id_available,
            wait_time_secs =wait_time_secs)

        broker_order_id_list = []

        #Case 1: Where Instrument_is is available directly
        if instrument_id_available:
            for idx, each_order in self.events_and_actions.orderbook.iterrows():
                broker_order_id = self.broker.place_market_order( \
                    instrument_id=each_order['instrument_id_trade'],
                    buy_sell=each_order['buy_sell'],
                    quantity=each_order['quantity'],
                    exchange=each_order['exchange'])
                broker_order_id_list.append(broker_order_id)
                self.events_and_actions.orderbook.drop(idx, inplace=True)
                self.total_trade_fee += self.per_trade_fee
                logger1.log(total_trade_fee = self.total_trade_fee)

        # Case 2: Where instrument_id is not available 
        #   First fetch instrument id using:
        #       Strike
        #       call_put
        #       expiry_datetime
        else:
            for idx, each_order in self.events_and_actions.orderbook.iterrows():
                instrument_id = self.broker.get_fno_instrument_id(broker_for='trade',
                                                                    strike=each_order['strike'],
                                                                    underlying=self.data_guy.underlying_name,
                                                                    call_put=each_order['call_put'],
                                                                    expiry_datetime=each_order['expiry_datetime']
                                                                    )
                broker_order_id = self.broker.place_market_order(instrument_id=instrument_id,
                                                                    buy_sell=each_order['buy_sell'],
                                                                    quantity=each_order['quantity'])
                broker_order_id_list.append(broker_order_id)
                self.events_and_actions.orderbook.drop(idx, inplace=True)
                self.total_trade_fee += self.per_trade_fee
                logger1.log(total_trade_fee = self.total_trade_fee)

        is_order_successful = True

        #Check if all orders are executed
        for each_broker_order_id in broker_order_id_list:
            each_broker_order_success = self.broker.is_order_complete(each_broker_order_id)

            is_order_successful = is_order_successful & each_broker_order_success

        t0 = perf_counter()
        current_wait_time = 0

        #if all orders are not successful wait for the set wait time
        while (not is_order_successful) & (
                current_wait_time < wait_time_secs):  # wait till orders are successful or wait time is over
            is_order_successful = True
            for each_broker_order_id in broker_order_id_list:
                is_order_successful = is_order_successful & self.broker.is_order_complete(each_broker_order_id)
            current_wait_time = perf_counter() - t0

        #After all orders are successful or wait time is over
        #   return all order status True or False 
        return is_order_successful


    @keep_log(default_return=(None,None))
    def strike_discovery(self, underlying, call_put, expiry_datetime, \
                         based_on_value, value, range_from_atm=2000) -> (int,float):
        """
        Scans through multiple options and returns best suits Strike which has:
            Price or Delta closest to Value
            For eg:
                - To find option haveing delta closest to .5
                    based_on_value: delta, value: 0.5
                - To find option prices closest to ₹ 3
                    based_on_value: price, value: 3
        Scanned options will be filtered based on:
            underlying name
            call_put
            expiry_datetime
            range_from_atm: 
                For eg if range_from_atm=2000
                    and spot = 18_180
                    Strike range will be 16_180 to 20_180

        Args:
            underlying (str): underlying name
            call_put(str): CE/PE
            expiry_datetime(datetime.datetime): expiry
            based_on_value: price/delta
            value (float): options closest to value
            range_from_atm (float): range from ATM to search best suited strike from

        Returns:
            (strike(int),price(float)): tuple containing strike and price of discovered option
        """        

        # if based_on_value is price, 
        # change it to instrument_ltp,
        # to follow the column naming convention
        if based_on_value == 'price':
            based_on_value = 'instrument_ltp'

            # Get a list of available strikes from broker
        available_strikes_from_broker = self.broker.get_available_strikes( \
            underlying=underlying, call_put=call_put,
            expiry_datetime=expiry_datetime)
        # Generate a list of numbers based on range_from_atm
        available_strikes_from_range = [*range(int(self.data_guy.current_ltp) - range_from_atm, \
                                                int(self.data_guy.current_ltp) + range_from_atm + 1, 1)]
        # List of available strikes based on intersection of
        #   strikes from Broker and range from ATM
        available_strikes = list(set(available_strikes_from_range) \
                                    .intersection(set(available_strikes_from_broker)))

        #create FnO_df based on available strike
        fno_df = pd.DataFrame({
            'underlying': underlying,
            'call_put': call_put,
            'expiry_datetime': expiry_datetime,
            'strike': [available_strikes]})
        fno_df = fno_df.explode('strike').reset_index()

        #Get instrument id for trade broker of all options
        fno_df['instrument_id_trade'] = self.broker.get_multiple_fno_instrument_id( \
            fno_df=fno_df, broker_for='trade')

        #Get instrument id for data broker of all options
        fno_df['instrument_id_data'] = self.broker.get_multiple_fno_instrument_id( \
            fno_df=fno_df, broker_for='data')

        #Get LTP of all options
        fno_df['instrument_ltp'] = self.broker.get_multiple_ltp(fno_df, exchange='NFO')

        #Calculate delta for options
        if based_on_value=='delta':
            fno_df = self.data_guy.calculate_greeks(fno_df, greek_type='delta', inplace=False)

            # fno_df.to_csv(f'''interim_df/calculate_delta_{datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}.csv''',
            #                 index=False)

        fno_df['value'] = value
        fno_df['minimize'] = abs(fno_df[based_on_value] - fno_df['value'])

        #find strike with value closest to based_on_value
        strike = fno_df[fno_df['minimize'] == fno_df['minimize'].min()]['strike'].iloc[0]
        instrument_ltp = fno_df[fno_df['minimize'] == fno_df['minimize'].min()]['instrument_ltp'].iloc[0]

        return strike, instrument_ltp


    @keep_log()
    def get_positions(self) -> pd.DataFrame:
        """
        Fetch current positions from Broker

        Returns:
            positions(pd.DataFrame): Current positions in form of DataFrame
        """        
        positions = self.broker.get_positions()
        return positions


class Algo_manager:
    """
    Algo_manager class is designed to initiate and set parameters 
    for all objects: data_guy, events_and_actions, trader and broker
    Action function is available which is meant to act on every iteration,
    currently it updates data_guy and checks for events_and_actions.
    """    
    def __init__(self, broker_for_trade, underlying_name, \
                 broker_for_data,
                 per_trade_fee=0,
                 log_folder="logs",
                 begin_time=datetime(2020, 1, 1, 9, 28).time(), \
                 close_time=datetime(2020, 1, 1, 15, 7).time(), \
                 trailing_loss_trigger_per_lot=1_500,
                 max_trailing_loss_non_expiry_per_lot=-250,
                 max_trailing_loss_expiry_per_lot = -200,
                 total_loss_limit_per_lot=-1_500,
                 candle_length=5,
                 quantity_per_lot = 50,
                 lots_traded = 1,
                 non_expiry_day_no_candle_time = datetime(2020, 1, 1, 14, 30).time(),
                 expiry_day_no_candle_time = datetime(2020, 1, 1, 13, 0).time(),
                 kite_api_key=None, kite_access_token=None,
                 kotak_consumer_key=None, kotak_access_token=None,
                 kotak_consumer_secret=None, kotak_user_id=None,
                 kotak_access_code=None, kotak_user_password=None,
                 current_datetime=None) -> None:

        """Initialize Algo_manager by passing paramaters
            Args:
                Broker:
                    broker_for_trade: broker name used for trade, 
                    broker_for_data: broker name used to get data, 
                    kite_api_key: kite_api_key, 
                    kite_access_token: kite_access_token, 
                    kotak_consumer_key: kotak_consumer_key, 
                    kotak_user_id: kotak_user_id, 
                    kotak_access_token: kotak_access_token, 
                    kotak_consumer_secret: kotak_consumer_secret, 
                    kotak_user_password: kotak_user_password, 
                    kotak_access_code: kotak_access_code,
                    current_datetime: current_datetime,

                Data_guy:
                    underlying_name: underlying_name, 
                    current_datetime: current_datetime,
                    candle_length: length of candle to be used for analysis,

                Events_and_actions:
                    begin_time: time when we start trades, 
                    close_time: time when we stop trading 
                        and square-off all positions,
                    lots_traded: Number of lots to be traded,
                    quantity_per_lot: quantity in one lot,
                    total_loss_limit_per_lot: total loss capacity per lot
                        triggers tradding stop and square-off,
                    trailing_loss_trigger_per_lot: trailing_loss 
                        capacity per lot
                        triggers tradding stop and square-off,
                    max_trailing_loss_per_lot_non_expiry: max_trailing_loss 
                        capacity per lot 
                        for non expiry day
                        triggers tradding stop and square-off,
                    max_trailing_loss_expiry: max_trailing_loss capacity per lot 
                        for non expiry day
                        triggers tradding stop and square-off,
                    non_expiry_day_no_candle_time: time after which 
                        candle will not be referred 
                        before taking position 
                        for non expiry day,
                    expiry_day_no_candle_time: time after which 
                        candle will not be referred 
                        before taking position 
                        for expiry day,
                
                Trader:
                    per_trade_fee: Brokerage charge per trade, 
                    
        """                 

        #if current_datetime is not provided default is to now()
        if current_datetime is None: current_datetime = datetime.now()

        trade_quantity = quantity_per_lot * lots_traded
        
        #set loss parameters for total lots
        max_trailing_loss_non_expiry = max_trailing_loss_non_expiry_per_lot * lots_traded
        max_trailing_loss_expiry = max_trailing_loss_expiry_per_lot * lots_traded
        total_loss_limit = total_loss_limit_per_lot * lots_traded
        trailing_loss_trigger = trailing_loss_trigger_per_lot * lots_traded

        #Initiating all objects Broker, data_guy, events_andactions and trader
        self.broker = Broker()
        self.data_guy = Data_guy()
        self.events_and_actions = Events_and_actions()
        self.trader = Trader()

        #recall the global logger1
        global logger1
        #setting up global variable logger1
        logger1 = Logger(is_logging=True,
                    broker_for_trade=broker_for_trade, \
                    broker_for_data=broker_for_data, \
                    log_folder=log_folder, \
                    trading_date=current_datetime.date(),\
                    data_guy = self.data_guy)
        logger1.log(info="Logger Initiated")
        
        #setting parameters for broker
        self.broker.set_parameters( \
            broker_for_trade=broker_for_trade, \
            broker_for_data=broker_for_data, \
            kite_api_key=kite_api_key, \
            kite_access_token=kite_access_token, \
            kotak_consumer_key=kotak_consumer_key, \
            kotak_user_id=kotak_user_id, \
            kotak_access_token=kotak_access_token, \
            kotak_consumer_secret=kotak_consumer_secret, \
            kotak_user_password=kotak_user_password, \
            kotak_access_code=kotak_access_code,
            current_datetime=current_datetime,
            logger = logger1,
            data_guy= self.data_guy)

        #setting parameters for data_guy
        self.data_guy.set_parameters( \
            broker=self.broker, \
            trader=self.trader,
            underlying_name=underlying_name, \
            current_datetime=current_datetime,
            candle_length=candle_length,
            events_and_actions = self.events_and_actions)

        #setting parameters for events_and_action
        self.events_and_actions.set_parameters( \
            data_guy=self.data_guy, \
            begin_time=begin_time, \
            close_time=close_time, \
            total_loss_limit=total_loss_limit,
            trade_quantity=trade_quantity, \
            trailing_loss_trigger=trailing_loss_trigger,
            max_trailing_loss_non_expiry=max_trailing_loss_non_expiry,
            max_trailing_loss_expiry=max_trailing_loss_expiry,
            non_expiry_day_no_candle_time = non_expiry_day_no_candle_time,
            expiry_day_no_candle_time = expiry_day_no_candle_time,
            trader=self.trader
        )

        #setting parameters for trader
        self.trader.set_parameters(broker=self.broker, \
                             per_trade_fee=per_trade_fee, \
                             data_guy=self.data_guy,
                             events_and_actions=self.events_and_actions,
                             )


    @keep_log()
    def action(self, current_datetime=None) -> None:
        """
        Combine all steps required in each iteration.
        Currently:
            1) Update data_guy
            2) Check for all events_and_actions
        Args:
            current_datetime (datetime.datetime, optional): Enter current datetime. Defaults to None.
        """        

        #if current datetime is not provided, default it to now()
        if current_datetime is None: current_datetime = datetime.now()

        #Step 1: Update data for current_datetime
        self.data_guy.update_data(current_datetime=current_datetime)
        
        #Step 2: Check all events_and_actions
        self.events_and_actions.events_to_actions()



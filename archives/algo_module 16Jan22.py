import pandas as pd
from datetime import datetime, timedelta
from time import perf_counter
from py_vollib.black_scholes.implied_volatility import implied_volatility as bs_iv
from py_vollib.black_scholes.greeks import analytical as greeks
from broker_module import Broker
import logging
from pandas.core.arrays import boolean


class Data_guy:
    def __init__(self, broker, trader, underlying_name,
                 logger, current_datetime=None, \
                 fixed_candle_minutes=None) -> None:
        self.logger = logger
        if current_datetime is None: current_datetime = datetime.now()
        self.current_datetime = current_datetime
        self.current_pnl = 0
        self.strategy_pnl = 0
        self.brokerage_pnl = 0
        self.max_pnl = 0
        self.trailing_pnl = 0
        self.current_ltp = None
        self.data_df = pd.DataFrame()
        timestamp_string = datetime.now().strftime("%Y-%m-%d %H_%M_%S")
        self.data_df_store_path = f'./Data_df/Data {timestamp_string} team.csv'
        self.data_df.to_csv(self.data_df_store_path, index=False)
        self.broker = broker
        self.trader = trader
        self.underlying_name = underlying_name.upper()
        self.orderbook = pd.DataFrame()
        self.expiry_datetime = self.broker.get_next_expiry_datetime(
            self.underlying_name)  # Expiry date provided by Data Broker
        self.is_expiry_day = False
        self.events_and_actions = None
        self.fixed_candle_minutes = fixed_candle_minutes
        self.class_name_dict_for_logger = {'className': self.__class__.__name__}
        self.logger.info( \
            f"|{current_datetime.date()}|{current_datetime.time()}| data guy Current_Date:{self.current_datetime}, ExpiryDatetime: {self.expiry_datetime}", \
            extra=self.class_name_dict_for_logger)
        if self.expiry_datetime.date() == self.current_datetime.date(): self.is_expiry_day = True

        self.logger.info( \
            f"|{current_datetime.date()}|{current_datetime.time()}| data guy initiated", \
            extra=self.class_name_dict_for_logger)
        

    def set_trader(self, trader) -> None:
        self.logger.info( \
            f"|{self.current_datetime.date()}|\
                        {self.current_datetime.time()}|\
                        set trader function called", \
            extra=self.class_name_dict_for_logger)
        try:
            self.trader = trader

            self.logger.info( \
                f"|{self.current_datetime.date()}|\
                            {self.current_datetime.time()}|\
                            set trader function called", \
                extra=self.class_name_dict_for_logger)
        except Exception as e:
            self.logger.info( \
                f"|{self.current_datetime.date()}|\
                            {self.current_datetime.time()}|\
                            Exception in set trader function: {e}", \
                extra=self.class_name_dict_for_logger)

    def set_events_and_actions(self, events_and_actions) -> None:
        self.logger.info( \
            f"|{self.current_datetime.date()}|\
                        {self.current_datetime.time()}|\
                        set trader function called", \
            extra=self.class_name_dict_for_logger)
        try:
            self.events_and_actions = events_and_actions

            self.logger.info( \
                f"|{self.current_datetime.date()}|\
                            {self.current_datetime.time()}|\
                            set events_and_actions function called", \
                extra=self.class_name_dict_for_logger)
        except Exception as e:
            self.logger.info( \
                f"|{self.current_datetime.date()}|\
                            {self.current_datetime.time()}|\
                            Exception in set events_and_actions function: {e}", \
                extra=self.class_name_dict_for_logger)

    def update_data(self, current_datetime=None) -> boolean:

        if current_datetime is None: current_datetime = datetime.now()
        self.logger.info( \
            f"|{current_datetime.date()}|{current_datetime.time()}| Update Data Called", \
            extra=self.class_name_dict_for_logger)

        try:
            self.current_datetime = current_datetime

            self.current_pnl = self.broker.get_pnl() + self.trader.total_trade_fee
            self.strategy_pnl = self.broker.get_pnl()
            self.brokerage_pnl = self.trader.total_trade_fee
            if self.current_pnl > self.max_pnl: self.max_pnl = self.current_pnl
            self.logger.info( \
                f"|{current_datetime.date()}|{current_datetime.time()}| PNL -{self.current_pnl} - {self.max_pnl}", \
                extra=self.class_name_dict_for_logger)
            self.trailing_pnl = self.current_pnl - self.max_pnl
            self.current_ltp = self.broker.get_ltp(self.underlying_name)
            candle_5_5_open = self.fixed_candle_value(5, -5, 'open')
            candle_5_5_close = self.fixed_candle_value(5, -5, 'close')
            candle_5_0_open = self.fixed_candle_value(5, 0, 'open')
            candle_5_0_close = self.fixed_candle_value(5, 0, 'close')
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
                                                'is_close': self.events_and_actions.is_close,\
                                                'candle_5_5_open':candle_5_5_open,\
                                                'candle_5_5_close':candle_5_5_close,\
                                                'candle_5_0_open':candle_5_0_open,
                                                'candle_5_0_close':candle_5_0_close}, \
                                               ignore_index=True)

            self.data_df.to_csv(self.data_df_store_path, index=False)

            update_successful = True
            if update_successful:
                self.logger.info( \
                    f"|{current_datetime.date()}|{current_datetime.time()}| data updated->{self.current_ltp}", \
                    extra=self.class_name_dict_for_logger)
        except Exception as e:
            self.logger.info( \
                f"|{current_datetime.date()}|{current_datetime.time()}|Exception while updating data: {e}", \
                extra=self.class_name_dict_for_logger)
            update_successful = False

        return update_successful

    def get_atm_strike(self) -> int:

        self.logger.info( \
            f"|{self.current_datetime.date()}|{self.current_datetime.time()}| Get ATM Strike", \
            extra=self.class_name_dict_for_logger)
        try:
            atm_strike = 50 * round(self.current_ltp / 50)
            self.logger.info( \
                f"|{self.current_datetime.date()}|{self.current_datetime.time()}|ATM Strike->{atm_strike}", \
                extra=self.class_name_dict_for_logger)
        except Exception as e:
            self.logger.info( \
                f"|{self.current_datetime.date()}|{self.current_datetime.time()}|Exception while updating data: {e}", \
                extra=self.class_name_dict_for_logger)
            update_successful = False
            atm_strike = None
        return atm_strike

    def fixed_candle_value(self, duration_minutes, relative_end_time_minutes=0, value_type='close') -> float:

        self.logger.info( \
            f"|{self.current_datetime.date()}|{self.current_datetime.time()}|\
                        Fixed Candle Value-{duration_minutes}-{relative_end_time_minutes}-{value_type}", \
            extra=self.class_name_dict_for_logger)

        try:
            if len(self.data_df) == 0:
                return None
            end_datetime = self.current_datetime + timedelta(minutes=relative_end_time_minutes)
            end_datetime_fixed = end_datetime - (end_datetime - datetime.min) % timedelta(minutes=duration_minutes)
            start_datetime_fixed = end_datetime_fixed - timedelta(minutes=duration_minutes)

            candle_df = self.data_df[(self.data_df['current_datetime'] >= start_datetime_fixed) \
                                     & (self.data_df['current_datetime'] <= end_datetime_fixed)] \
                                    ['current_ltp']
            if len(candle_df) >= 2:
                if value_type == 'close':
                    candle_value = candle_df.iloc[-1]
                elif value_type == 'open':
                    candle_value = candle_df.iloc[0]
                elif value_type == 'high':
                    candle_value = candle_df.max()
                elif value_type == 'low':
                    candle_value = candle_df.min()
            else:
                candle_value = None

            self.logger.info( \
                f"|{self.current_datetime.date()}|{self.current_datetime.time()}|\
                        Fixed Candle Value-{start_datetime_fixed}-{end_datetime_fixed}-{value_type}->{candle_value}", \
                extra=self.class_name_dict_for_logger)
        except Exception as e:
            self.logger.info( \
                f"|{self.current_datetime.date()}|{self.current_datetime.time()}|\
                    Candle Value-{duration_minutes}-{relative_end_time_minutes}-{value_type}\
                    ->Exception while getting candle value: {e}", \
                extra=self.class_name_dict_for_logger)
            candle_value = None
        return candle_value

    def candle_value(self, duration_minutes, relative_end_time_minutes=0, value_type='close') -> float:

        self.logger.info( \
            f"|{self.current_datetime.date()}|{self.current_datetime.time()}|\
                        Candle Value-{duration_minutes}-{relative_end_time_minutes}-{value_type}", \
            extra=self.class_name_dict_for_logger)
        try:
            end_time = self.current_datetime + timedelta(minutes=relative_end_time_minutes)
            start_time = end_time - timedelta(minutes=duration_minutes)

            candle_df = self.data_df[(self.data_df['current_datetime'] >= start_time) \
                                     & (self.data_df['current_datetime'] <= end_time)] \
                ['current_ltp']

            if len(candle_df) >= 2:
                if value_type == 'close':
                    candle_value = candle_df.iloc[-1]
                elif value_type == 'open':
                    candle_value = candle_df.iloc[0]
                elif value_type == 'high':
                    candle_value = candle_df.max()
                elif value_type == 'low':
                    candle_value = candle_df.min()
            else:
                candle_value = None

            self.logger.info( \
                f"|{self.current_datetime.date()}|{self.current_datetime.time()}|\
                        Candle Value-{duration_minutes}-{relative_end_time_minutes}-{value_type}->{candle_value}", \
                extra=self.class_name_dict_for_logger)
        except Exception as e:
            self.logger.info( \
                f"|{self.current_datetime.date()}|{self.current_datetime.time()}|\
                    Candle Value-{duration_minutes}-{relative_end_time_minutes}-{value_type}\
                    ->Exception while getting candle value: {e}", \
                extra=self.class_name_dict_for_logger)
            candle_value = None
        return candle_value

    def calculate_greeks(self, df, greek_type='delta', risk_free_rate=.07, inplace=True, filter_iv=1):
        ## df should have expiry_datetime, call_put, instrument_ltp and strike columns

        self.logger.info( \
            f"|{self.current_datetime.date()}|{self.current_datetime.time()}|\
                        calculate greeks-{greek_type}-{risk_free_rate}->{df.to_json()}", \
            extra=self.class_name_dict_for_logger)

        if not inplace:
            df = df.copy()
            df.to_csv(f'''interim_df/received_by_greek_calculator{datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}.csv''')

        df['time_to_expiry_years'] = (self.expiry_datetime \
                                      - self.current_datetime).total_seconds() \
                                     / timedelta(days=364).total_seconds()

        call_put_pyvollib_map = {'CE': 'c', 'PE': 'p'}
        df['call_put_pyvollib'] = df['call_put'].map(call_put_pyvollib_map)

        def calculate_iv(each_row):
            iv_value = 0
            try:
                iv_value = bs_iv(price=each_row['instrument_ltp'], S=self.current_ltp,
                                 K=each_row['strike'], t=each_row['time_to_expiry_years'],
                                 r=risk_free_rate, flag=each_row['call_put_pyvollib'])

            except Exception as e:
                # self.logger.info(\
                #     f"|{self.current_datetime.date()}|{self.current_datetime.time()}|\
                #         Error in calculating IV {each_row.to_json()}: {e}", \
                #     extra=self.class_name_dict_for_logger)
                iv_value = None
            return iv_value

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

                # self.logger.info(\
                #     f"|{self.current_datetime.date()}|{self.current_datetime.time()}|\
                #         Error in calculating {greek_type} {each_row.to_json()}: {e}", \
                #     extra=self.class_name_dict_for_logger)

                greek_value = None
            return greek_value

        df['implied_volatility'] = df.apply(calculate_iv, axis=1)
        df.to_csv(f'''interim_df/calculated_IV{datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}.csv''')
        df = df[df['implied_volatility'] < filter_iv]
        df.to_csv(f'''interim_df/filtered_iv_above_{filter_iv}-{datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}.csv''')
        df[greek_type] = df.apply(calculate_greek, axis=1)
        df.to_csv(f'''interim_df/calculated-{greek_type}-{datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}.csv''')

        df.drop(columns=['call_put_pyvollib', 'time_to_expiry_years'], inplace=True)

        self.logger.info( \
            f"|{self.current_datetime.date()}|{self.current_datetime.time()}|\
                        calculated greeks-{greek_type}-{risk_free_rate}->{df.to_json()}", \
            extra=self.class_name_dict_for_logger)
        df.to_csv(f'''interim_df/calculate_delta_{datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}.csv''', index=False)
        return df


class Events_and_actions:

    def __init__(self, data_guy, trader, logger, \
                 begin_time=datetime(2020, 1, 1, 9, 28).time(), \
                 close_time=datetime(2020, 1, 1, 15, 7).time(), \
                 total_loss_limit=-1_000, trade_quantity=50, \
                 trailing_loss_trigger=1_500, max_trailing_loss=-250) -> None:
        self.logger = logger
        self.is_hedged = False
        self.current_position = None
        self.straddle_strike = None
        self.strangle_strike_high = None
        self.strangle_strike_low = None
        self.is_close = False
        self.begin_time = begin_time
        self.close_time = close_time
        self.position_entry_ltp = None
        self.total_loss_limit = total_loss_limit
        self.trade_quantity = trade_quantity
        self.trailing_loss_trigger = trailing_loss_trigger
        self.max_trailing_loss = max_trailing_loss
        self.orderbook = pd.DataFrame()
        self.data_guy = data_guy
        self.trader = trader
        self.class_name_dict_for_logger = {'className': self.__class__.__name__}
        self.logger.info( \
            f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        events and actions initiated", \
            extra=self.class_name_dict_for_logger)

    def log_event_as_warning(self):
        self.logger.warning( \
            f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        Event", \
            extra=self.class_name_dict_for_logger)

    def display_string(self):

        try:
            display_string = f"{self.data_guy.current_ltp}\t{self.data_guy.current_pnl}\t"

            if self.current_position is None:
                display_string += f"-\to\t-"
            elif self.current_position == 'straddle':
                display_string += f"{self.straddle_strike}\tDAL\t{self.straddle_strike}"
            elif self.current_position == 'strangle':
                display_string += f"{self.strangle_strike_low}\tGLE\t{self.strangle_strike_high}"

            return display_string
        except Exception as e:
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        Exception: {e}", \
                extra=self.class_name_dict_for_logger)

    def events_to_actions(self):

        self.logger.info( \
            f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        events and actions function called", \
            extra=self.class_name_dict_for_logger)

        self.logger.info( \
            f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        At_Start->\
                        is_closed:{self.is_close},\
                        is_hedged:{self.is_hedged},\
                        current_position:{self.current_position},\
                        straddle_strike:{self.straddle_strike},\
                        strangle_Strike_high:{self.strangle_strike_high},\
                        strangle_strike_low:{self.strangle_strike_low}".replace(" ", ""), \
            extra=self.class_name_dict_for_logger)

        try:
            if self.event_total_loss_reached():
                self.log_event_as_warning()
                self.action_close_the_day()
            elif self.event_trailing_loss_reached():
                self.log_event_as_warning()
                self.action_close_the_day()
            elif self.event_expiry_day_trailing_loss_reached():
                self.log_event_as_warning()
                self.action_close_the_day()
            elif self.event_shop_close_time():
                self.log_event_as_warning()
                self.action_close_the_day()
            elif self.event_exit_straddle():
                self.log_event_as_warning()
                self.action_exit_position()
            elif self.event_exit_strangle():
                self.log_event_as_warning()
                self.action_exit_position()
            elif self.event_expiry_day_enter_position_call_first():
                self.log_event_as_warning()
                self.action_enter_position_call_first()
            elif self.event_expiry_day_enter_position_put_first():
                self.log_event_as_warning()
                self.action_enter_position_put_first()
            elif self.event_non_expiry_day_enter_position_call_first():
                self.log_event_as_warning()
                self.action_enter_position_call_first()
            elif self.event_non_expiry_day_enter_position_put_first():
                self.log_event_as_warning()
                self.action_enter_position_put_first()
            elif self.event_enter_position_call_first():
                self.log_event_as_warning()
                self.action_enter_position_call_first()
            elif self.event_enter_position_put_first():
                self.log_event_as_warning()
                self.action_enter_position_put_first()
            elif self.event_expiry_day_open_shop():
                self.log_event_as_warning()
                self.action_expiry_day_buy_hedge()
            elif self.event_non_expiry_day_open_shop():
                self.log_event_as_warning()
                self.action_non_expiry_day_buy_hedge()

            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                            {self.data_guy.current_datetime.time()}|\
                            events and actions function complete", \
                extra=self.class_name_dict_for_logger)

            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        At_End->\
                        is_closed:{self.is_close},\
                        is_hedged:{self.is_hedged},\
                        current_position:{self.current_position},\
                        straddle_strike:{self.straddle_strike},\
                        strangle_Strike_high:{self.strangle_strike_high},\
                        strangle_strike_low:{self.strangle_strike_low},\
                        position_entry_ltp:{self.position_entry_ltp}".replace(" ", ""), \
                extra=self.class_name_dict_for_logger)
        except Exception as e:
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                            {self.data_guy.current_datetime.time()}|\
                            Exception in events and actions function: {e}", \
                extra=self.class_name_dict_for_logger)

    def set_trader(self, trader) -> None:
        self.logger.info( \
            f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        set trader function called", \
            extra=self.class_name_dict_for_logger)
        try:
            self.trader = trader

            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                            {self.data_guy.current_datetime.time()}|\
                            set trader function called", \
                extra=self.class_name_dict_for_logger)
        except Exception as e:
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                            {self.data_guy.current_datetime.time()}|\
                            Exception in set trader function: {e}", \
                extra=self.class_name_dict_for_logger)

    def event_expiry_day_open_shop(self) -> boolean:

        self.logger.info( \
            f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        called", \
            extra=self.class_name_dict_for_logger)
        try:
            output = False
            if (self.data_guy.is_expiry_day) & (not self.is_close) & (not self.is_hedged):
                if self.data_guy.current_datetime.time() >= self.begin_time:
                    output = True

            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                    {self.data_guy.current_datetime.time()}|\
                    ->{output}", \
                extra=self.class_name_dict_for_logger)

            return output
        except Exception as e:
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                    {self.data_guy.current_datetime.time()}|\
                    Exception: {e}", \
                extra=self.class_name_dict_for_logger)
            return False

    def event_non_expiry_day_open_shop(self) -> boolean:

        self.logger.info( \
            f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        event_non_expiry_day_open_shop called", \
            extra=self.class_name_dict_for_logger)

        try:
            output = False
            if (not self.data_guy.is_expiry_day) & (not self.is_close) & (not self.is_hedged):
                if self.data_guy.current_datetime.time() >= self.begin_time:
                    output = True

            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                    {self.data_guy.current_datetime.time()}|\
                    event_non_expiry_day_open_shop->{output}", \
                extra=self.class_name_dict_for_logger)

            return output
        except Exception as e:
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                    {self.data_guy.current_datetime.time()}|\
                    Exception in event_non_expiry_day_open_shop: {e}", \
                extra=self.class_name_dict_for_logger)

        return False

    def event_enter_position_call_first(self) -> boolean:

        self.logger.info( \
            f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        event_enter_position_call_first called", \
            extra=self.class_name_dict_for_logger)

        try:
            output = False
            if (not self.is_close) & self.is_hedged & (self.current_position == None):
                candle_5_5_open = self.data_guy.fixed_candle_value(5, -5, 'open')
                candle_5_5_close = self.data_guy.fixed_candle_value(5, -5, 'close')
                if candle_5_5_open is not None and candle_5_5_close is not None:
                    if abs(candle_5_5_open - candle_5_5_close) < 15:
                        candle_5_0_open = self.data_guy.fixed_candle_value(5, 0, 'open')
                        candle_5_0_close = self.data_guy.fixed_candle_value(5, 0, 'close')
                        if candle_5_0_open is not None and candle_5_0_close is not None:
                            if abs(candle_5_0_open - candle_5_0_close) < 15:
                                if abs(candle_5_0_close-self.data_guy.current_ltp) < 5:
                                    if self.data_guy.current_ltp >= self.data_guy.get_atm_strike():
                                        output = True

            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        event_enter_position_call_first->{output}", \
                extra=self.class_name_dict_for_logger)

            return output
        except Exception as e:
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        Exception in event_enter_position_call_first:{e}", \
                extra=self.class_name_dict_for_logger)
            return False

    def event_enter_position_put_first(self) -> boolean:

        self.logger.info( \
            f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        event_enter_position_put_first called", \
            extra=self.class_name_dict_for_logger)
        try:

            output = False

            if (not self.is_close) & self.is_hedged & (self.current_position == None):
                candle_5_5_open = self.data_guy.fixed_candle_value(5, -5, 'open')
                candle_5_5_close = self.data_guy.fixed_candle_value(5, -5, 'close')
                if candle_5_5_open is not None and candle_5_5_close is not None:
                    if abs(candle_5_5_open - candle_5_5_close) < 15:
                        candle_5_0_open = self.data_guy.fixed_candle_value(5, 0, 'open')
                        candle_5_0_close = self.data_guy.fixed_candle_value(5, 0, 'close')
                        if abs(candle_5_0_open - candle_5_0_close) < 15:
                            if abs(candle_5_0_close - self.data_guy.current_ltp) < 5:
                                if self.data_guy.current_ltp < self.data_guy.get_atm_strike():
                                    output = True

            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        event_enter_position_put_first->{output}", \
                extra=self.class_name_dict_for_logger)
            return output
        except Exception as e:
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        Exception in event_enter_position_put_first: {e}", \
                extra=self.class_name_dict_for_logger)
            return False

    def event_exit_straddle(self) -> boolean:

        self.logger.info( \
            f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        event_exit_straddle called", \
            extra=self.class_name_dict_for_logger)

        try:
            output = False
            if self.current_position == 'straddle':
                if abs(self.data_guy.current_ltp - self.position_entry_ltp) > 10:
                    ltp_minus_strike = self.data_guy.current_ltp - self.straddle_strike
                    if ltp_minus_strike < -27 or ltp_minus_strike > 27:
                        output = True
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        event_exit_straddle->{output}", \
                extra=self.class_name_dict_for_logger)
            return output
        except Exception as e:

            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        Exception in event_exit_straddle:{e}", \
                extra=self.class_name_dict_for_logger)
            return False

    def event_exit_strangle(self) -> boolean:

        self.logger.info( \
            f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        event_exit_strangle called", \
            extra=self.class_name_dict_for_logger)
        try:
            output = False
            if self.current_position == 'strangle':
                if abs(self.data_guy.current_ltp - self.position_entry_ltp) > 10:
                    ltp_minus_high_strike = self.data_guy.current_ltp - self.strangle_strike_high
                    ltp_minus_low_strike = self.data_guy.current_ltp - self.strangle_strike_low
                    if ltp_minus_high_strike < -52 or ltp_minus_low_strike > 52:
                        output = True
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        event_exit_strangle->{output}", \
                extra=self.class_name_dict_for_logger)
            return output
        except Exception as e:
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        Exception in event_exit_strangle:{e}", \
                extra=self.class_name_dict_for_logger)
        return False

    def event_expiry_day_enter_position_call_first(self) -> boolean:

        self.logger.info( \
            f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        called", \
            extra=self.class_name_dict_for_logger)
        try:
            output = False
            if (not self.is_close) & self.is_hedged & (self.current_position == None):
                if (self.data_guy.is_expiry_day) & (
                        self.data_guy.current_datetime.time() >= datetime(2020, 1, 1, 13, 0).time()):
                    if self.data_guy.current_ltp >= self.data_guy.get_atm_strike():
                        output = True
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        ->{output}", \
                extra=self.class_name_dict_for_logger)
            return output
        except Exception as e:
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        :{e}", \
                extra=self.class_name_dict_for_logger)
            return False

    def event_expiry_day_enter_position_put_first(self) -> boolean:

        self.logger.info( \
            f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        called", \
            extra=self.class_name_dict_for_logger)

        try:
            output = False
            if (not self.is_close) & self.is_hedged & (self.current_position == None):
                if (self.data_guy.is_expiry_day) & (
                        self.data_guy.current_datetime.time() >= datetime(2020, 1, 1, 13, 0).time()):
                    if self.data_guy.current_ltp < self.data_guy.get_atm_strike():
                        output = True
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        ->{output}", \
                extra=self.class_name_dict_for_logger)
            return output
        except Exception as e:
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        Exception:{e}", \
                extra=self.class_name_dict_for_logger)
            return False

    def event_non_expiry_day_enter_position_call_first(self) -> boolean:

        self.logger.info( \
            f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        event_non_expiry_day_enter_position_call_first called", \
            extra=self.class_name_dict_for_logger)
        try:
            output = False
            if (not self.is_close) & self.is_hedged & (self.current_position == None):
                if (not self.data_guy.is_expiry_day) & (
                        self.data_guy.current_datetime.time() >= datetime(2020, 1, 1, 14, 30).time()):
                    if self.data_guy.current_ltp >= self.data_guy.get_atm_strike():
                        output = True
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        event_non_expiry_day_enter_position_call_first->{output}", \
                extra=self.class_name_dict_for_logger)

            return output
        except Exception as e:
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        Exception in event_non_expiry_day_enter_position_call_first:{e}", \
                extra=self.class_name_dict_for_logger)
            return False

    def event_non_expiry_day_enter_position_put_first(self) -> boolean:

        self.logger.info( \
            f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        event_non_expiry_day_enter_position_put_first called", \
            extra=self.class_name_dict_for_logger)

        try:
            output = False

            if (not self.is_close) & self.is_hedged & (self.current_position == None):
                if (not self.data_guy.is_expiry_day) & (
                        self.data_guy.current_datetime.time() >= datetime(2020, 1, 1, 14, 30).time()):
                    if self.data_guy.current_ltp < self.data_guy.get_atm_strike():
                        output = True

            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        event_non_expiry_day_enter_position_put_first->{output}", \
                extra=self.class_name_dict_for_logger)
            return output
        except Exception as e:
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        Exception in event_non_expiry_day_enter_position_put_first:{e}", \
                extra=self.class_name_dict_for_logger)

            return False

    def event_total_loss_reached(self) -> boolean:

        self.logger.info( \
            f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        event_total_loss_reached called", \
            extra=self.class_name_dict_for_logger)
        try:
            output = False
            if not self.is_close:
                if self.data_guy.current_pnl < self.total_loss_limit:
                    output = True
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        event_total_loss_reached->{output}", \
                extra=self.class_name_dict_for_logger)
            return output
        except Exception as e:
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        Exception in event_total_loss_reached:{e}", \
                extra=self.class_name_dict_for_logger)
            return False

    def event_shop_close_time(self) -> boolean:

        self.logger.info( \
            f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        event_shop_close_time called", \
            extra=self.class_name_dict_for_logger)
        try:
            output = False
            if not self.is_close:
                if self.data_guy.current_datetime.time() >= self.close_time:
                    output = True
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        event_shop_close_time->{output}", \
                extra=self.class_name_dict_for_logger)
            return output
        except Exception as e:
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        Exception in event_shop_close_time: {e}", \
                extra=self.class_name_dict_for_logger)
            return False

    def event_trailing_loss_reached(self) -> boolean:

        self.logger.info( \
            f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        called", \
            extra=self.class_name_dict_for_logger)
        try:
            output = False
            if not self.is_close:
                if (self.data_guy.max_pnl >= self.trailing_loss_trigger) & (
                        self.data_guy.trailing_pnl <= self.max_trailing_loss):
                    output = True
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        {output}", \
                extra=self.class_name_dict_for_logger)
            return output
        except Exception as e:
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        Exception {e}", \
                extra=self.class_name_dict_for_logger)
        return False

    def event_expiry_day_trailing_loss_reached(self) -> boolean:

        self.logger.info( \
            f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        called", \
            extra=self.class_name_dict_for_logger)
        try:
            output = False
            if not self.is_close:
                if (self.data_guy.current_datetime.time() >= datetime(2020, 1, 1, 14, 50).time()):
                    if (self.data_guy.max_pnl >= 1_000) & (self.data_guy.trailing_pnl <= -200):
                        output = True
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        {output}", \
                extra=self.class_name_dict_for_logger)
            return output
        except Exception as e:
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        Exception:{e}", \
                extra=self.class_name_dict_for_logger)
            return False

    def action_expiry_day_buy_hedge(self) -> boolean:

        self.logger.info( \
            f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        called", \
            extra=self.class_name_dict_for_logger)

        try:
            output = False

            strike, _ = self.trader.strike_discovery(underlying=self.data_guy.underlying_name,
                                                     call_put='CE', expiry_datetime=self.data_guy.expiry_datetime,
                                                     based_on_value='price', value=3)
            order = {'order_id': str(datetime.now()), \
                     'strike': strike, \
                     'underlying': self.data_guy.underlying_name, \
                     'call_put': 'CE', \
                     'expiry_datetime': self.data_guy.expiry_datetime, \
                     'quantity': self.trade_quantity, \
                     'buy_sell': 'buy'}
            self.orderbook = self.orderbook.append(order, ignore_index=True)

            strike, _ = self.trader.strike_discovery(underlying=self.data_guy.underlying_name,
                                                     call_put='PE', expiry_datetime=self.data_guy.expiry_datetime,
                                                     based_on_value='price', value=3)
            order = {'order_id': str(datetime.now()), \
                     'strike': strike, \
                     'underlying': self.data_guy.underlying_name, \
                     'call_put': 'PE', \
                     'expiry_datetime': self.data_guy.expiry_datetime, \
                     'quantity': self.trade_quantity,
                     'buy_sell': 'buy'}
            self.orderbook = self.orderbook.append(order, ignore_index=True)

            orders_executed = self.trader.place_order_in_orderbook()

            if orders_executed:
                self.is_hedged = True
                output = True

            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        {output}", \
                extra=self.class_name_dict_for_logger)
            return output
        except Exception as e:
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        Exception: {e}", \
                extra=self.class_name_dict_for_logger)

            return False

    def action_non_expiry_day_buy_hedge(self) -> boolean:

        self.logger.info( \
            f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        called", \
            extra=self.class_name_dict_for_logger)

        try:
            output = False
            strike, _ = self.trader.strike_discovery(underlying=self.data_guy.underlying_name,
                                                     call_put='CE', expiry_datetime=self.data_guy.expiry_datetime,
                                                     based_on_value='price', value=5)
            order = {'order_id': str(datetime.now()), 'strike': strike, 'underlying': self.data_guy.underlying_name,
                     'call_put': 'CE', 'expiry_datetime': self.data_guy.expiry_datetime,
                     'quantity': self.trade_quantity,
                     'buy_sell': 'buy'}
            self.orderbook = self.orderbook.append(order, ignore_index=True)

            strike, _ = self.trader.strike_discovery(underlying=self.data_guy.underlying_name,
                                                     call_put='PE', expiry_datetime=self.data_guy.expiry_datetime,
                                                     based_on_value='price', value=5)
            order = {'order_id': str(datetime.now()), 'strike': strike, 'underlying': self.data_guy.underlying_name,
                     'call_put': 'PE', 'expiry_datetime': self.data_guy.expiry_datetime,
                     'quantity': self.trade_quantity,
                     'buy_sell': 'buy'}
            self.orderbook = self.orderbook.append(order, ignore_index=True)

            orders_executed = self.trader.place_order_in_orderbook()

            if orders_executed:
                self.is_hedged = True
                output = True

            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        {output}", \
                extra=self.class_name_dict_for_logger)

        except Exception as e:
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        Exception:{e}", \
                extra=self.class_name_dict_for_logger)
            return False

    def action_enter_position_put_first(self) -> boolean:

        self.logger.info( \
            f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        called", \
            extra=self.class_name_dict_for_logger)

        try:
            ouput = False
            strike_put, instrument_ltp = self.trader.strike_discovery(underlying=self.data_guy.underlying_name,
                                                                      call_put='PE',
                                                                      expiry_datetime=self.data_guy.expiry_datetime,
                                                                      based_on_value='delta', value=-.5)
            order = {'order_id': str(datetime.now()), 'strike': strike_put, 'underlying': self.data_guy.underlying_name,
                     'call_put': 'PE', 'expiry_datetime': self.data_guy.expiry_datetime,
                     'quantity': self.trade_quantity,
                     'buy_sell': 'sell'}
            self.orderbook = self.orderbook.append(order, ignore_index=True)

            strike_call, _ = self.trader.strike_discovery(underlying=self.data_guy.underlying_name,
                                                          call_put='CE', expiry_datetime=self.data_guy.expiry_datetime,
                                                          based_on_value='price', value=instrument_ltp)
            order = {'order_id': str(datetime.now()), 'strike': strike_call,
                     'underlying': self.data_guy.underlying_name,
                     'call_put': 'CE', 'expiry_datetime': self.data_guy.expiry_datetime,
                     'quantity': self.trade_quantity,
                     'buy_sell': 'sell'}
            self.orderbook = self.orderbook.append(order, ignore_index=True)

            orders_executed = self.trader.place_order_in_orderbook()

            if orders_executed:
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
                self.logger.info( \
                    f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        {output}", \
                    extra=self.class_name_dict_for_logger)
            return output
        except Exception as e:
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        Exception:{e}", \
                extra=self.class_name_dict_for_logger)
            return False

    def action_enter_position_call_first(self) -> boolean:

        self.logger.info( \
            f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        called", \
            extra=self.class_name_dict_for_logger)

        try:
            output = False
            strike_call, instrument_ltp = self.trader.strike_discovery( \
                underlying=self.data_guy.underlying_name,
                call_put='CE', expiry_datetime=self.data_guy.expiry_datetime,
                based_on_value='delta', value=.5)
            order = {'order_id': str(datetime.now()),
                     'strike': strike_call,
                     'underlying': self.data_guy.underlying_name,
                     'call_put': 'CE',
                     'expiry_datetime': self.data_guy.expiry_datetime,
                     'quantity': self.trade_quantity,
                     'buy_sell': 'sell'}
            self.orderbook = self.orderbook.append(order, ignore_index=True)

            strike_put, _ = self.trader.strike_discovery( \
                underlying=self.data_guy.underlying_name,
                call_put='PE', expiry_datetime=self.data_guy.expiry_datetime,
                based_on_value='price', value=instrument_ltp)
            order = {'order_id': str(datetime.now()),
                     'strike': strike_put,
                     'underlying': self.data_guy.underlying_name,
                     'call_put': 'PE',
                     'expiry_datetime': self.data_guy.expiry_datetime,
                     'quantity': self.trade_quantity,
                     'buy_sell': 'sell'}
            self.orderbook = self.orderbook.append(order, ignore_index=True)

            orders_executed = self.trader.place_order_in_orderbook()

            if orders_executed:
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

                self.logger.info( \
                    f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        {output}", \
                    extra=self.class_name_dict_for_logger)

                return output

        except Exception as e:
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        Exception:{e}", \
                extra=self.class_name_dict_for_logger)

            return False

    def action_exit_position(self) -> boolean:

        self.logger.info( \
            f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        called", \
            extra=self.class_name_dict_for_logger)

        try:
            output = False
            current_position = self.trader.get_positions()
            current_position = current_position[ \
                current_position['quantity'] < 0] \
                .sort_values(by='quantity')

            # ADD ORDERS TO EXIT ALL POSITION

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

            orders_executed = self.trader.place_order_in_orderbook(instrument_id_available=True)

            if orders_executed:
                self.current_position = None
                self.straddle_strike = None
                self.strangle_strike_high = None
                self.strangle_strike_low = None
                self.position_entry_ltp = None
                output = True

            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        {output}", \
                extra=self.class_name_dict_for_logger)

            return output

        except Exception as e:
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        Exception: {e}", \
                extra=self.class_name_dict_for_logger)
            return False

    def action_close_the_day(self) -> boolean:

        self.logger.info( \
            f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        called", \
            extra=self.class_name_dict_for_logger)

        try:
            output = False
            current_position = self.trader.get_positions()

            if len(current_position) != 0:
                current_position = current_position[ \
                    current_position['quantity'] != 0] \
                    .sort_values(by='quantity')
                # ADD ORDERS TO EXIT ALL POSITION

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

            orders_executed = self.trader.place_order_in_orderbook(instrument_id_available=True)

            if orders_executed:
                self.current_position = None
                self.straddle_strike = None
                self.strangle_strike_high = None
                self.strangle_strike_low = None
                self.position_entry_ltp = None
                self.is_hedged = False
                self.is_close = True
                output = True

            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        {output}", \
                extra=self.class_name_dict_for_logger)

            return output
        except Exception as e:
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        Exception:{e}", \
                extra=self.class_name_dict_for_logger)
            return False


class Trader:
    def __init__(self, broker, data_guy, events_and_actions, logger, per_trade_fee=0) -> None:

        self.logger = logger
        self.broker = broker
        self.data_guy = data_guy
        self.events_and_actions = events_and_actions
        self.tradebook = pd.DataFrame()
        self.current_positionbook = pd.DataFrame()
        self.per_trade_fee = per_trade_fee
        self.total_trade_fee = 0
        self.class_name_dict_for_logger = {'className': self.__class__.__name__}
        self.logger.info( \
            f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}\
                            | trader initiated", \
            extra=self.class_name_dict_for_logger)

        pass

    def place_order_in_orderbook(self, wait_time_secs=3, instrument_id_available=False) -> boolean:

        self.logger.info( \
            f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        Placing Order-{self.events_and_actions.orderbook.to_json()}-\
                        {instrument_id_available}-\
                        {wait_time_secs}", \
            extra=self.class_name_dict_for_logger)

        try:
            broker_order_id_list = []

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

                pass
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

            is_order_successful = True

            for each_broker_order_id in broker_order_id_list:
                each_broker_order_success = self.broker.is_order_complete(each_broker_order_id)

                is_order_successful = is_order_successful & each_broker_order_success

            t0 = perf_counter()
            current_wait_time = 0

            while (not is_order_successful) & (
                    current_wait_time < wait_time_secs):  # wait till orders are successful or wait time is over
                is_order_successful = True
                for each_broker_order_id in broker_order_id_list:
                    is_order_successful = is_order_successful & self.broker.is_order_complete(each_broker_order_id)
                current_wait_time = perf_counter() - t0

            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        Placed Orders->{is_order_successful}-\
                        {self.events_and_actions.orderbook.to_json()}-\
                        {instrument_id_available}-\
                        {wait_time_secs}", \
                extra=self.class_name_dict_for_logger)

            return is_order_successful

        except Exception as e:
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        Exception in Placing Order:{e}-{self.events_and_actions.orderbook.to_json()}-\
                        {instrument_id_available}-\
                        {wait_time_secs}", \
                extra=self.class_name_dict_for_logger)
            return False

    def strike_discovery(self, underlying, call_put, expiry_datetime, \
                         based_on_value, value, range_from_atm=2000):

        self.logger.info( \
            f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        Strike Discovery Entered-{underlying}-{call_put}-{expiry_datetime}-\
                        {based_on_value}-{value}-{range_from_atm}".replace(" ", ""), \
            extra=self.class_name_dict_for_logger)

        try:
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

            fno_df = pd.DataFrame({
                'underlying': underlying,
                'call_put': call_put,
                'expiry_datetime': expiry_datetime,
                'strike': [available_strikes]})
            fno_df = fno_df.explode('strike').reset_index()

            fno_df['instrument_id_trade'] = self.broker.get_multiple_fno_instrument_id( \
                fno_df=fno_df, broker_for='trade')

            fno_df['instrument_id_data'] = self.broker.get_multiple_fno_instrument_id( \
                fno_df=fno_df, broker_for='data')

            fno_df['instrument_ltp'] = self.broker.get_multiple_ltp(fno_df, exchange='NFO')

            fno_df.to_csv(f'''interim_df/sending_to\
                        calculate_delta_{datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}.csv''' \
                          .replace(" ", "")
                          , index=False)

            fno_df = self.data_guy.calculate_greeks(fno_df, greek_type='delta', inplace=False)

            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        Strike Discovery - Greeks Calculated->{fno_df.to_json()}".replace(" ", ""), \
                extra=self.class_name_dict_for_logger)

            fno_df.to_csv(f'''interim_df/calculate_delta_{datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}.csv''',
                          index=False)

            fno_df['value'] = value
            fno_df['minimize'] = abs(fno_df[based_on_value] - fno_df['value'])

            strike = fno_df[fno_df['minimize'] == fno_df['minimize'].min()]['strike'].iloc[0]
            instrument_ltp = fno_df[fno_df['minimize'] == fno_df['minimize'].min()]['instrument_ltp'].iloc[0]

            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        Strike Discovery-{underlying}-{call_put}-{expiry_datetime}-\
                        {based_on_value}-{value}-{range_from_atm}->Strike:{strike}, LTP: {instrument_ltp}".replace(" ",
                                                                                                                   ""), \
                extra=self.class_name_dict_for_logger)

            return strike, instrument_ltp

        except Exception as e:
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        Exception in Strike Discovery:{e}-{underlying}-{call_put}-{expiry_datetime}-\
                        {based_on_value}-{value}-{range_from_atm}".replace(" ", ""), \
                extra=self.class_name_dict_for_logger)
            return None, None

    def get_positions(self) -> pd.DataFrame:

        self.logger.info( \
            f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        Get Positions Called", \
            extra=self.class_name_dict_for_logger)

        try:
            positions = self.broker.get_positions()

            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        ->{positions.to_json()}", \
                extra=self.class_name_dict_for_logger)

            return positions

        except Exception as e:

            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        Exception {e}", \
                extra=self.class_name_dict_for_logger)

            return None


class Algo_manager:
    def __init__(self, broker_for_trade, underlying_name, \
                 broker_for_data,
                 per_trade_fee=0,
                 log_folder="logs",
                 begin_time=datetime(2020, 1, 1, 9, 28).time(), \
                 close_time=datetime(2020, 1, 1, 15, 7).time(), \
                 trailing_loss_trigger=1_500,
                 max_trailing_loss=-250,
                 total_loss_limit=-1_500, trade_quantity=50,
                 fixed_candle_minutes=5,
                 kite_api_key=None, kite_access_token=None,
                 kotak_consumer_key=None, kotak_access_token=None,
                 kotak_consumer_secret=None, kotak_user_id=None,
                 kotak_access_code=None, kotak_user_password=None,
                 current_datetime=None) -> None:

        if current_datetime is None: current_datetime = datetime.now()
        self.class_name_dict_for_logger = {'className': self.__class__.__name__}

        self.logger = self.initialize_logger(broker_for_trade=broker_for_trade, \
                                             broker_for_data=broker_for_data, \
                                             log_folder=log_folder, \
                                             simulation_date=current_datetime.date())

        self.logger.info( \
            f"| | |Logger Initiated", \
            extra=self.class_name_dict_for_logger)

        self.broker = Broker( \
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
            logger=self.logger,
            current_datetime=current_datetime)

        self.logger.info( \
            f"| | |Broker Initiated", \
            extra=self.class_name_dict_for_logger)
        self.logger.info( \
            f"| | |Next Expiry Date:{self.broker.get_next_expiry_datetime(underlying_name)}", \
            extra=self.class_name_dict_for_logger)

        self.data_guy = Data_guy( \
            broker=self.broker, \
            trader=None,
            underlying_name=underlying_name, \
            current_datetime=current_datetime,
            fixed_candle_minutes=fixed_candle_minutes, \
            logger=self.logger)

        self.logger.info( \
            f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        Data Guy Initiated", \
            extra=self.class_name_dict_for_logger)

        self.events_and_actions = Events_and_actions( \
            data_guy=self.data_guy, \
            begin_time=begin_time, \
            close_time=close_time, \
            total_loss_limit=total_loss_limit,
            trade_quantity=trade_quantity, \
            trailing_loss_trigger=trailing_loss_trigger,
            max_trailing_loss=max_trailing_loss,
            trader=None, logger=self.logger
        )

        self.logger.info( \
            f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        events and actions Initiated", \
            extra=self.class_name_dict_for_logger)

        self.trader = Trader(broker=self.broker, \
                             per_trade_fee=per_trade_fee, \
                             data_guy=self.data_guy,
                             events_and_actions=self.events_and_actions,
                             logger=self.logger)

        self.logger.info( \
            f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        Trader initiated", \
            extra=self.class_name_dict_for_logger)

        self.data_guy.set_trader(trader=self.trader)
        self.data_guy.set_events_and_actions(events_and_actions=self.events_and_actions)
        self.events_and_actions.set_trader(trader=self.trader)

    def action(self, current_datetime=None) -> None:

        if current_datetime is None: current_datetime = datetime.now()
        self.logger.info( \
            f"|{current_datetime.date()}|\
                        {current_datetime.time()}|\
                        Action Called", \
            extra=self.class_name_dict_for_logger)

        try:

            self.data_guy.update_data(current_datetime=current_datetime)
            self.events_and_actions.events_to_actions()
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        Action Complete", \
                extra=self.class_name_dict_for_logger)
        except Exception as e:
            self.logger.info( \
                f"|{self.data_guy.current_datetime.date()}|\
                        {self.data_guy.current_datetime.time()}|\
                        Exception: {e}", \
                extra=self.class_name_dict_for_logger)

    def initialize_logger(self, broker_for_trade, broker_for_data, log_folder="Logs", simulation_date=None):

        
        if simulation_date is None: simulation_date = datetime.now().date()
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(levelname)s|%(className)s->%(funcName)s|%(message)s')

        timestamp_string = datetime.now().strftime("%Y-%m-%d %H_%M_%S")
        file_handler = logging.FileHandler(
            f'./{log_folder}/T-{broker_for_trade} D-{broker_for_data} {simulation_date} @ {timestamp_string} team.log')
        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)

        return logger

import pandas as pd
import numpy as np
from time import perf_counter


class Quant:

    def calculate_greeks():
        pass

    def calculate_iv():
        pass

    pass

class Algo:

    def prepare_orderbook():
        pass

    def analyse_market():
        pass

    pass

class Trader:

    def __init__(self, broker_object, order_execution_wait_time) -> None:
        self.broker_object = broker_object
        self.order_execution_wait_time = order_execution_wait_time
        pass

    def place_orders(self, orderbook):
        orderbook.sort_values(by='priority', inplace=True)
        for index, each_order in orderbook.iterrows():
            order_id = self.broker_object.place_order(underlying = each_order['underlying'], 
                            strike = each_order['strike'],expiry = each_order['expiry'],
                            option_type = each_order['option_type'], quantity=each_order['quantity'])
            t0 = perf_counter()
            while not (self.broker_object.order_executed(order_id)) or (perf_counter() - t0 > self.order_execution_wait_time):
                pass
            if not self.broker_object.order_executed(order_id):
                #  Enter code in case orders are not executed  
                pass

        pass


class Risk_manager:

    def __init__(self, entry_time, exit_time, broker_object,max_loss, underlying) -> None:
        self.timestamp = None
        self.active = False
        self.underlying = underlying
        self.time_section = "before"
        self.time_status = 0
        self.pnl_status = 1
        self.movement_status = 1
        self.underlying_upper_bound = 1_00_000
        self.underlying_lower_bound = 0 
        self.entry_time = entry_time
        self.exit_time = exit_time
        self.broker_object = broker_object
        self.running_pnl = None
        if max_loss > 0:
            self.max_loss = max_loss * -1
        else:
            self.max_loss = max_loss
        pass

    def check_time(self,timestamp) -> bool:
        self.timestamp = timestamp
        if self.time_section == "before":
            if self.timestamp >= self.entry_time:
                self.time_section = "active"
                self.time_status = 1
        elif self.time_status=="active":
            if self.timestamp >= self.exit_time:
                self.time_section = "after"
                self.time_status = 0
        return self.update_running_status()


    def pnl_risk(self) -> bool:
        self.running_pnl = self.broker_object.get_running_pnl()
        if self.running_pnl <= self.max_loss:
            self.pnl_status = 0
        return self.update_running_status()
        

    def movement_risk(self) -> bool:
        underlying_ltp = self.broker_object.get_ltp(self.underlying)
        if (underlying_ltp >= self.underlying_upper_bound) or (underlying_ltp <= self.underlying_lower_bound):
            self.movement_status = 0
        return self.update_running_status()

    def update_running_status(self) -> bool:
        self.active = bool(self.time_status * self.pnl_status * self.movement_status)
        return self.active
    pass


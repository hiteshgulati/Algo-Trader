from datetime import datetime
from time import sleep
import logging
from functools import wraps

logger = None

class Logger ():

    def __init__(self, is_logging, is_timing, data_guy= None, broker_for_trade="0", broker_for_data="0", log_folder="Logs", simulation_date=None) -> None:
        if simulation_date is None: simulation_date = datetime.now().date()
        self.is_logging = is_logging
        self.is_timing = is_timing
        self.logger = None
        if self.is_logging:
            self.logger = logging.getLogger(__name__)
            self.logger.setLevel(logging.INFO)
            formatter = logging.Formatter('%(levelname)s|%(className)s->%(functionName)s|%(message)s')
            # formatter = logging.Formatter('%(levelname)s|%(message)s')

            timestamp_string = datetime.now().strftime("%Y-%m-%d %H_%M_%S")
            file_handler = logging.FileHandler(
                f'./{log_folder}/T-{broker_for_trade} D-{broker_for_data} {simulation_date} @ {timestamp_string} team.log')
            file_handler.setFormatter(formatter)

            self.logger.addHandler(file_handler)

        self.log_levels = {'CRITICAL':50,
                           'ERROR':40,
                           'WARNING':30,
                           'INFO':20,
                           'DEBUG':10,
                           'NOTSET':0}
        
        self.data_guy = data_guy

    def set_data_guy(self,data_guy):
        self.data_guy = data_guy

    def log (self,extra=None,**kwargs):
        
        if self.is_logging:
            logger_prefix_string = ""
            if extra is None:
                extra = {\
                    'className': "",
                    'functionName': ""}
            if 'level' in kwargs:
                numeric_log_level = \
                    self.log_levels[kwargs['level'].upper()]
            else: 
                numeric_log_level = \
                    self.log_levels['INFO']

            if self.data_guy is not None:
                current_datetime_string = f"\
                    {self.data_guy.current_datetime.date()} \
                        | \
                    {self.data_guy.current_datetime.time()} \
                        | "
                current_datetime_string = " ".\
                        join(current_datetime_string.\
                            split())
            else: 
                current_datetime_string= " | | "
            
            logger_prefix_string = logger_prefix_string + current_datetime_string
            
            for key,value in kwargs.items():
                key_value_string = f"{key}-{value};"
                logger_prefix_string = \
                    logger_prefix_string + key_value_string

            self.logger.log(level=numeric_log_level,
                msg=f"|{logger_prefix_string}",
                extra=extra)


def keep_log (**kwargs_decorator):
    def decorator_function (original_function):
        @wraps(original_function)
        def warpper_function(*args,**kwargs):
            class_function_name_dict = {\
                    'className': args[0].__class__.__name__,
                    'functionName': original_function.__name__}
            logger.log(status="Called",new_info='BAZINGA',kill_bill='Uma',extra=class_function_name_dict,**kwargs_decorator)
            try:
                result = original_function(*args,**kwargs)
                logger.log(result=result,extra=class_function_name_dict,status="End",**kwargs_decorator)
                return result
            except Exception as e:
                logger.log(status="Exception",exception=e,extra=class_function_name_dict,**kwargs_decorator)
        return warpper_function
    return decorator_function


class some_class:
    def __init__(self) -> None:
        pass
    @keep_log()
    def display(self,name,k):
        print(f"{name}")
        logger.log(status="Mid", level='error')
        a = 1/k

if __name__ == '__main__':
    logger = Logger(is_logging=True,is_timing=False,log_folder="play logs")

    k = some_class()
    print(datetime.now())
    sleep(1)
    k.display("Hitesh",1)
    print(datetime.now())
    sleep(2)
    k.display("Hitesh",0)
    print(datetime.now())
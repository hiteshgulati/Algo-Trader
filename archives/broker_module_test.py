from broker_module import Broker
from kiteconnect import KiteConnect
from datetime import datetime
import pandas as pd


from ks_api_client import ks_api
from datetime import timedelta


kotak_access_token = "d2c0a033-86d3-3b66-9b94-7241b0078fef"
kotak_user_id = "HG11011990"
kotak_consumer_secret = "PGLsaajJLkpKLFXNO6pXrjCr_6Qa"
kotak_consumer_key = "ug1QtNucZF4WLSinsfGfQ0sgjoYa"
kotak_password = "KSSh@r3d"
kotak_access_code = "1970"

broker = Broker(broker="kotak", kotak_consumer_key=kotak_consumer_key, kotak_access_token=kotak_access_token,
                kotak_consumer_secret=None,kotak_user_id=kotak_user_id,
                kotak_access_code=None, kotak_user_password=kotak_password)

expiry_datetime = broker.get_next_expiry_datetime()

broker.get_pnl()  
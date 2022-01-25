from kiteconnect import KiteConnect
import os

def generate_access_token(request_token=None,token_folder_path=None) -> str:
        # if not token_folder_path:
        #         token_folder_path = "/Users/hg/OneDrive/Code/zerodha tokens"
        

        # key_secret = open(os.path.join(token_folder_path,"api_key.txt"),'r').read().split()
        # kite = KiteConnect(api_key=key_secret[0])


        api_key = "5ytarhiur9g1jebq"

        api_secret = "q3hkmxt8b1gf4ftj5ew9x455sejb409h"

        kite = KiteConnect(api_key=api_key)

        if not request_token:
                return kite.login_url() # Use this URL to manually login and authorize yourself

        else:
        #generate trading session
                data = kite.generate_session(request_token=request_token, api_secret=api_secret)

                return data['access_token']


if __name__ == "__main__":
        # print(generate_access_token())
        request_token='7S3vHRDpdpsZ09XxtGVfWa1PXL4HJV10'
        print(generate_access_token(request_token=request_token))
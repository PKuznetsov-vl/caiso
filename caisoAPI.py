import atexit
import io
import os
import time
import zipfile
import datetime
import requests
import pandas as pd
import logging
from tqdm import tqdm
from tenacity import retry, stop_after_attempt, retry_if_exception_type

logging.basicConfig(filename='logerrors.txt', level=logging.INFO)


class Caiso:
    def __init__(self):
        """login"""
        self.nodename = None
        self.sess = requests.Session()

    # если не заработает retry_if_exception_type то retry_if_result
    @retry(stop=stop_after_attempt(3), retry=retry_if_exception_type(requests.exceptions.HTTPError))
    def get_prices(self, startdate, enddate):
        """ЧТО делает функция
        Note: check that you are connecting via USA
                 :return: smth
                     :param startdate: smth
                     :param enddate: smth
                  """
        retry_delay = 6  # seconds
        rsp = self.sess.get(
            f'http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_INTVL_LMP&startdatetime={startdate}&'
            f'enddatetime={enddate}&version=1&market_run_id=RTM&node={self.nodename}&resultformat=6',
            timeout=10)
        try:
            rsp.raise_for_status()
            z = zipfile.ZipFile(io.BytesIO(rsp.content))
            # TODO usecols,set data type
            df = pd.read_csv(z.open(z.namelist()[0]))
            # df = pd.read_csv(csv)
        except (requests.exceptions.HTTPError) as e:
            if e.response.status_code == 429:
                time.sleep(retry_delay)
            else:
                logging.error(
                    f"request for {startdate}-{enddate} {self.nodename} in market_run_id=RTM: Error occurred: {str(e)}")
                logging.error("Note: check that you are connecting via USA")
                return f"Error: {e}"

        return df[["INTERVALSTARTTIME_GMT", "INTERVALENDTIME_GMT", "NODE", "MARKET_RUN_ID", "LMP_TYPE", "MW",
                   "OPR_INTERVAL"]]  # как то странно возвращается лучше usecols в начале

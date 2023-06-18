import io
import time
import zipfile
import threading
import requests
import logging
import pandas as pd
from tenacity import retry, retry_if_exception_type, wait_fixed, stop_after_attempt, RetryError

class Caiso:
    """
    Class for interacting with the Caiso API.

    Attributes:
        __ATTEMPTS (int): Maximum number of retry attempts for API requests.
        __DELAY (int): Delay in seconds between retry attempts.
        __REFRESH_TIME (int): Time in seconds between session refreshes.
        __slots__ (list): List of attribute names allowed in instances of Caiso.

    Methods:
        __init__(self, sess=None, nodename=None):
            Initialize the Caiso object with a session and nodename.
        start_session_refresh(self):
            Start a thread to refresh the session periodically.
        get_prices(self, startdate, enddate):
            Retrieve price data from the Caiso API for the specified date range.
        get_dam(self, startdate, enddate):
            Retrieve DAM (Day-Ahead Market) data from the Caiso API for the specified date range.
        __setattr__(self, name, value):
            Set attribute value if it's a valid attribute.
        __repr__(self):
            Return a string representation of the Caiso object.
        __str__(self):
            Return a string description of the Caiso object.            
    """

    __ATTEMPTS = 3
    __DELAY = 6
    __REFRESH_TIME = 300
    __slots__ = ["sess", "logger", "nodename"]
    
    def __init__(self, sess=None, logger=None):
        """
        Initialize the Caiso object with a session and nodename.

        :param sess: Requests session object (optional)
        :type sess: requests.Session, optional
        :param nodename: Name of the node (optional)
        :type nodename: str, optional
        """
        self.sess = sess or requests.Session()
        if logger is None:
            self.logger = logging.getLogger(__name__)
            logging.basicConfig(filename='logs.txt', level=logging.INFO)
        else:
            self.logger = logger
        self.nodename = None

    def start_session_refresh(self):
        """Start a thread to refresh the session periodically."""
        refresh_thread = threading.Thread(target=self.__refresh_session, daemon=True)
        refresh_thread.start()

    def __refresh_session(self):
        """Refresh the session periodically."""
        while True:
            time.sleep(self.__REFRESH_TIME)
            self.sess = requests.Session()
            self.logger.info("Session successfully refreshed")

    @retry(retry=retry_if_exception_type(requests.exceptions.HTTPError), wait=wait_fixed(__DELAY), stop=stop_after_attempt(__ATTEMPTS))
    def __get_data(self, startdate, enddate, queryname, market_run_id):
        """
        Common logic to retrieve data from the Caiso API.

        :param startdate: Start date of the data.
        :type startdate: str
        :param enddate: End date of the data.
        :type enddate: str
        :param queryname: Name of the query.
        :type queryname: str
        :param market_run_id: Market run ID.
        :type market_run_id: str
        :return: DataFrame with the retrieved data.
        :rtype: pandas.DataFrame
        """
        if self.nodename is None:
            return None
        
        try:
            starttime = startdate.strftime('%Y%m%dT%H:%M-0000')
            endtime = enddate.strftime('%Y%m%dT%H:%M-0000')
            rsp = self.sess.get(
                f'http://oasis.caiso.com/oasisapi/SingleZip?queryname={queryname}&startdatetime={starttime}&enddatetime={endtime}&version=1&market_run_id={market_run_id}&node={self.nodename}&resultformat=6',
                timeout=10)
            rsp.raise_for_status()
            z = zipfile.ZipFile(io.BytesIO(rsp.content))
            df = pd.read_csv(z.open(z.namelist()[0]),
                            usecols=["INTERVALSTARTTIME_GMT",
                                     "INTERVALENDTIME_GMT",
                                     "NODE",
                                     "MARKET_RUN_ID",
                                     "LMP_TYPE",
                                     "MW",
                                     "OPR_INTERVAL"],
                            parse_dates=["INTERVALSTARTTIME_GMT", "INTERVALENDTIME_GMT"],
                            dtype={ "NODE": str,
                                    "MARKET_RUN_ID": str,
                                    "LMP_TYPE": str,
                                    "MW": float,
                                    "OPR_INTERVAL": int
                                    })
            return df
        except Exception as e:
            if not isinstance(e, requests.exceptions.HTTPError):
                if isinstance(e, requests.exceptions.ConnectionError):
                    logging.warning("You can only send requests from USA")
                logging.error(
                    f"Request for {startdate}-{enddate} {self.nodename} in market_run_id={market_run_id}: Error occurred: {str(e)}")
                return None

    def get_prices(self, startdate, enddate):
        """
        Retrieve price data from the Caiso API for the specified date range.

        :param startdate: Start date of the data.
        :type startdate: str
        :param enddate: End date of the data.
        :type enddate: str
        :return: DataFrame with the price data.
        :rtype: pandas.DataFrame
        """
        try:
            return self.__get_data(startdate, enddate, 'PRC_INTVL_LMP', 'RTM')
        except RetryError:
            return None
        
    def get_dam(self, startdate, enddate):
        """
        Retrieve DAM (Day-Ahead Market) data from the Caiso API for the specified date range.

        :param startdate: Start date of the data.
        :type startdate: str
        :param enddate: End date of the data.
        :type enddate: str
        :return: DataFrame with the DAM data.
        :rtype: pandas.DataFrame
        """
        try:
            return self.__get_data(startdate, enddate, 'PRC_LMP', 'DAM')
        except RetryError:
            return None
        
    def __setattr__(self, name, value):
        """Set attribute value if it's a valid attribute."""
        if name in self.__slots__ or name.startswith("_Caiso__"):
            super().__setattr__(name, value)
        else:
            raise AttributeError("Cannot add new attributes to Caiso objects")
        
    def __repr__(self):
        """Return a string representation of the Caiso object."""
        return f"Caiso(sess={self.sess}, nodename={self.nodename})"

    def __str__(self):
        """Return a string description of the Caiso object."""
        return f"Caiso object (nodename={self.nodename})"

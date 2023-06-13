import io
import os
import time
import zipfile
import datetime
import requests
import pandas as pd
import logging
from tqdm import tqdm
from tenacity import retry, stop_after_attempt
logging.basicConfig(filename='logerrors.txt', level=logging.INFO)

@retry(stop=stop_after_attempt(3))

def get_rtm_prices(nodename, startdate, enddate):
    retry_count = 3
    retry_delay = 6  # seconds
    try:
        rsp = requests.get(
            f'http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_INTVL_LMP&startdatetime={startdate}&'
            f'enddatetime={enddate}&version=1&market_run_id=RTM&node={nodename}&resultformat=6',
            timeout=10)

        rsp.raise_for_status()
        z = zipfile.ZipFile(io.BytesIO(rsp.content))
        csv = z.open(z.namelist()[0])
        df = pd.read_csv(csv)
    except (requests.HTTPError, zipfile.BadZipFile, requests.exceptions.Timeout) as e:
        if isinstance(e, requests.exceptions.Timeout) or isinstance(e, requests.HTTPError) \
                and e.response.status_code == 429:
            time.sleep(retry_delay)
        else:
            logging.error(
                f"request for {startdate}-{enddate} {nodename} in market_run_id=RTM: Error occurred: {e}")
            return None
    except requests.exceptions.ConnectionError as e:
        if 'ConnectionResetError' in str(e):
            time.sleep(retry_delay)
        else:
            logging.error(f"request for {startdate}-{enddate} {nodename} in market_run_id=RTM: {e}")
            logging.error("Note: check that you are connecting via USA")
            return None
    except Exception as e:
        logging.error(
            f"request for {startdate}-{enddate} {nodename} in market_run_id=RTM: Unhandled exception occurred: {str(e)}")
        return None

    return df[["INTERVALSTARTTIME_GMT", "INTERVALENDTIME_GMT", "NODE", "MARKET_RUN_ID", "LMP_TYPE", "MW",
                   "OPR_INTERVAL"]]


logging.error(f"request for {startdate}-{enddate} {nodename} in market_run_id=RTM: Exceeded maximum retry attempts")
return None


def get_dam(nodename, startdate, enddate):
    retry_count = 3
    retry_delay = 6  # seconds

    for _ in range(retry_count):
        try:
            rsp = requests.get(
                f'http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_LMP&startdatetime={startdate}&enddatetime={enddate}&version=1&market_run_id=DAM&node={nodename}&resultformat=6',
                timeout=10)

            rsp.raise_for_status()
            z = zipfile.ZipFile(io.BytesIO(rsp.content))
            csv = z.open(z.namelist()[0])
            df = pd.read_csv(csv)
        except (requests.HTTPError, zipfile.BadZipFile, requests.exceptions.Timeout) as e:
            if isinstance(e, requests.exceptions.Timeout) or isinstance(e,
                                                                        requests.HTTPError) and e.response.status_code == 429:
                time.sleep(retry_delay)
            else:
                logging.error(
                    f"request for {startdate}-{enddate} {nodename} in market_run_id=RTM: Error occurred: {str(e)}")
                return None
        except requests.exceptions.ConnectionError as e:
            if 'ConnectionResetError' in str(e):
                time.sleep(retry_delay)
            else:
                logging.error(f"request for {startdate}-{enddate} {nodename} in market_run_id=RTM: {str(e)}")
                logging.error("Note: check that you are connecting via USA")
                return None
        except Exception as e:
            logging.error(
                f"request for {startdate}-{enddate} {nodename} in market_run_id=RTM: Unhandled exception occurred: {str(e)}")
            return None
        else:
            return df[["INTERVALSTARTTIME_GMT", "INTERVALENDTIME_GMT", "NODE", "MARKET_RUN_ID", "LMP_TYPE", "MW"]]

    logging.error(f"request for {startdate}-{enddate} {nodename} in market_run_id=RTM: Exceeded maximum retry attempts")
    return None


if __name__ == '__main__':
    a=Caiso()
    for node in locations:
        a.Node_name=node
        a.get_prices()
    df = pd.read_csv('LMPLocations.csv')
    names_lst = df['name'].tolist()
    start_date = datetime.datetime(2022, 4, 30, 9, 0)
    end_date = datetime.datetime(2022, 5, 7, 0, 0)
    dates_list = list(start_date + datetime.timedelta(days=i) for i in range((end_date - start_date).days + 1))
    # dates_list = list(start_date + datetime.timedelta(days=7*i) for i in range((end_date - start_date).days // 7 + 1)) # если по неделям
    folder_path = 'data'
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    for node_name in names_lst:
        df_rtm = pd.DataFrame(
            columns=["INTERVALSTARTTIME_GMT", "INTERVALENDTIME_GMT", "NODE", "MARKET_RUN_ID", "LMP_TYPE", "MW",
                     "OPR_INTERVAL"])
        df_dam = pd.DataFrame(
            columns=["INTERVALSTARTTIME_GMT", "INTERVALENDTIME_GMT", "NODE", "MARKET_RUN_ID", "LMP_TYPE", "MW"])
        for i in tqdm(range(len(dates_list) - 1)): # todo enumerate здесь
            # разве date time не может конвертировать? например  now = date.fromisoformat(start_date)
            start_time = dates_list[i].strftime('%Y%m%dT%H:%M-0000')
            end_time = dates_list[i + 1].strftime('%Y%m%dT%H:%M-0000')

            df = get_prices(node_name, start_time, end_time)
            if df is not None:
                df_rtm = pd.concat([df_rtm, df], ignore_index=True)

            df = get_dam(node_name, start_time, end_time)
            if df is not None:
                df_dam = pd.concat([df_dam, df], ignore_index=True)

        if df_rtm.shape[0] > 0:
            file_path = f'{folder_path}/{node_name}-RTM-{start_date.strftime("%Y%m%d")}-{end_date.strftime("%Y%m%d")}.csv'
            with open(file_path, 'w', newline='') as file:
                df_rtm.to_csv(file, index=False)

        if df_dam.shape[0] > 0:
            file_path = f'{folder_path}/{node_name}-DAM-{start_date.strftime("%Y%m%d")}-{end_date.strftime("%Y%m%d")}.csv'
            with open(file_path, 'w', newline='') as file:
                df_dam.to_csv(file, index=False)

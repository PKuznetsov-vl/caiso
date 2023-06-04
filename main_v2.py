import io
import time
import zipfile
from datetime import datetime
import requests
import pandas as pd

def get_prices(nodename, startdate, enddate):
    retry_count = 3
    retry_delay = 6 # seconds

    for _ in range(retry_count):
        try:
            rsp = requests.get(
                f'http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_INTVL_LMP&startdatetime={startdate}&enddatetime={enddate}&version=1&market_run_id=RTM&node={nodename}&resultformat=6',
                timeout=335)

            rsp.raise_for_status()
            z = zipfile.ZipFile(io.BytesIO(rsp.content))
            csv = z.open(z.namelist()[0])
            df = pd.read_csv(csv)
            return df
        except (requests.HTTPError, zipfile.BadZipFile) as e:
            if isinstance(e, requests.HTTPError) and e.response.status_code == 429:
                time.sleep(retry_delay)
            else:
                print(f"Error occurred: {str(e)}")
                return None
    
    print("Exceeded maximum retry attempts")
    return None

def get_dam(nodename, startdate, enddate):
    retry_count = 3
    retry_delay = 6 # seconds

    for _ in range(retry_count):
        try:
            rsp = requests.get(
                f'http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_LMP&startdatetime={startdate}&enddatetime={enddate}&version=1&market_run_id=DAM&node={nodename}&resultformat=6')

            rsp.raise_for_status()
            z = zipfile.ZipFile(io.BytesIO(rsp.content))
            csv = z.open(z.namelist()[0])
            df = pd.read_csv(csv)
            return df
        except (requests.HTTPError, zipfile.BadZipFile) as e:
            if isinstance(e, requests.HTTPError) and e.response.status_code == 429:
                time.sleep(retry_delay)
            else:
                print(f"Error occurred: {str(e)}")
                return None
    
    print("Exceeded maximum retry attempts")
    return None


if __name__ == '__main__':
    df = pd.read_csv('LMPLocations.csv')
    names_lst = df['name'].tolist()
    current_date = datetime.datetime(2022, 8, 16, 9, 0) # время условное поставил для проверки,
    end_date = datetime.datetime(2022, 8, 25, 0, 0) # так-то можно поставить двухлетней давности и должно работать, просто долго

    while current_date <= end_date:
        date_str = current_date.strftime('%Y%m%d')
        
        for node_name in names_lst:
            start_time = current_date.strftime('%Y%m%dT%H:%M-0000')
            end_time = (current_date + datetime.timedelta(hours=15)).strftime('%Y%m%dT%H:%M-0000') # +15 часов, так как там данные с 9 утра до 00 след дня
            
            df = get_prices(node_name, start_time, end_time)
            if df is not None:
                df.to_csv(f'data/{node_name}-RTM-{date_str}.csv')
            
            # я нашел на сайте только для market_run_id=RTM (get_prices), поэтому возможно это не нужно
            # да и я почитал, RTM -- рынок, на котором определяются цены на энергию и осуществляются сделки в реальном времени.
            # т.е. получаем цены на энергию с рынка реального времени для указанного периода времени и местоположения.
            df = get_dam(node_name, start_time, end_time)
            if df is not None:
                df.to_csv(f'data/{node_name}-DAM-{date_str}.csv')
        
        current_date += datetime.timedelta(days=1)

import glob
import io
import os
import time
import zipfile
from dateutil import rrule
from datetime import datetime, timedelta, date
import requests
import pandas as pd

#TODO date to iso
from dotenv import load_dotenv


def get_date(start_date, numofdays: int):
    now = date.fromisoformat(start_date)
    end_date = now + timedelta(days=numofdays)

    listofdays = rrule.rrule(rrule.DAILY, dtstart=now, until=end_date)
    listofdays = list(map(str, listofdays))
    listofdays = list(
        map(lambda x: x.replace('-', '').replace(' ', 'T').rstrip(listofdays[0][14:]) + '00:00-0000', listofdays))
    return listofdays


def concate_all(path,out_path):
    all_files = glob.glob(os.path.join(path, "*.csv"))

    li = []
    print(all_files)
    for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0, low_memory=False)
        li.append(df)

    frame = pd.concat(li, axis=0, ignore_index=True)
    frame.to_csv(out_path, index=False)


def benchmark(func):
    def wrapper(*args, **kwargs):
        func(*args, **kwargs)
        print('success', *args)

    return wrapper


def get_prices(nodename, startdate, enddate):
    rsp = requests.get(
        f'http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_INTVL_LMP&startdatetime={startdate}&enddatetime={enddate}&version=1&market_run_id=RTM&node={nodename}&resultformat=6',
        timeout=335)

    z = zipfile.ZipFile(io.BytesIO(rsp.content))
    csv = z.open(z.namelist()[0])
    df = pd.read_csv(csv)
    return df


def get_dam(nodename, startdate, enddate):
    # http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_LMP&market_run_id=DAM&startdatetime=20210101T08%3A00-0000&enddatetime=20210102T08%3A00-0000&version=1&node=CAPTJACK_5_N003&resultformat=6

    rsp = requests.get(
        f'http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_LMP&startdatetime={startdate}&enddatetime={enddate}&version=1&market_run_id=DAM&node={nodename}&resultformat=6')
    z = zipfile.ZipFile(io.BytesIO(rsp.content))
    csv = z.open(z.namelist()[0])
    df = pd.read_csv(csv)
    return df


def get_node_info(name, dates, path):
    list_of_csv = []
    for day in range(len(dates) - 1):
        time.sleep(6)
        df = get_prices(name, dates[day], dates[day + 1])
        print(df.head())
        list_of_csv.append(df)
    print(len(list_of_csv))

    df_f = pd.concat(list_of_csv, ignore_index=True)
    print('Success')
    df_f.to_csv(f'{path}/{name}.csv', index=False)

""" Исползуйте подход указанный ниже чтобы переписать мой хардкод
def get_date(start_date, numofdays: int):
    now = date.fromisoformat(start_date)
    end_date = now + timedelta(days=numofdays)
    listofdays = rrule.rrule(rrule.DAILY, dtstart=now, until=end_date)
    


 это пример чтобы не подить большой массив
# Вывод 5 дней от даты старта
print(list(rrule.rrule(rrule.DAILY, count=5, dtstart=parse("20201202T090000"))))
 
# Вывод 3 дней от даты старта с интервалом 10
print(list(rrule.rrule(rrule.DAILY, interval=10, count=3, dtstart=parse("20201202T090000"))))
 
# Вывод 3 дней с интервалом неделя
print(list(rrule.rrule(rrule.WEEKLY, count=3, dtstart=parse("20201202T090000"))))
 
# Вывод 3 дней с интервалом месяц
print(list(rrule.rrule(rrule.MONTHLY, count=3, dtstart=parse("20201202T090000"))))
 
# Вывод 3 дней с интервалом год
print(list(rrule.rrule(rrule.YEARLY, count=3, dtstart=parse("20201202T090000"))))
    
    listofdays = list(map(lambda day: str(day.isoformat()) + 'Z', listofdays)) - с этой строки вам не понадобится
    # listofdays = list(map(listofdays, listofdays))
    # listofdays = list(
    #     map(lambda x: x.replace('-', '').replace(' ', 'T').rstrip(listofdays[0][14:]) + '00:00-0000', listofdays))
    return listofdays


"""
def get_all_nodes():
    df = pd.read_csv('LMPLocations.csv')
    names_lst = df['name'].tolist()

    for name in names_lst:
        time.sleep(6)
        df1 = get_prices(name, '20220401T00:00-0000', '20220415T00:00-0000')
        time.sleep(6)
        df2 = get_prices(name, '20220416T00:00-0000', '20220430T00:00-0000')
        time.sleep(6)
        df3 = get_prices(name, '20220501T00:00-0000', '20220715T00:00-0000')
        time.sleep(6)
        df4 = get_prices(name, '20220516T00:00-0000', '20220531T00:00-0000')
        time.sleep(6)
        df5 = get_prices(name, '20220601T00:00-0000', '20220615T00:00-0000')
        time.sleep(6)
        df6 = get_prices(name, '20220616T00:00-0000', '20220630T00:00-0000')
        time.sleep(6)
        df7 = get_prices(name, '20220701T00:00-0000', '20220715T00:00-0000')
        time.sleep(6)
        df8 = get_prices(name, '20220716T00:00-0000', '20220731T00:00-0000')
        df_f = pd.concat([df1, df2, df3, df4, df5, df6, df7, df8], ignore_index=True)
        print('Success')
        df_f.to_csv(f'csv/{name}.csv', index=False)


def get_node_DAM(name, dates, path):
    list_of_csv = []
    for day in range(len(dates) - 1):
        time.sleep(6)
        df = get_dam(name, dates[day], dates[day + 1])
        print(df.head())
        list_of_csv.append(df)
    print(len(list_of_csv))

    df_f = pd.concat(list_of_csv, ignore_index=True)
    print('Success')
    df_f.to_csv(f'{path}/{name}_DAM.csv', index=False)


def getco2(listofdates):
    for dt in listofdates:
        datetoreq = dt[:8]
        print(datetoreq)
        rsp = requests.get(f'https://www.caiso.com/outlook/SP/History/{datetoreq}/co2.csv?=1659725917675')
        df = pd.read_csv(io.StringIO(rsp.content.decode('utf-8')))
        df = df.rename(columns={'Time': 'Date'})

        df['Date'] = dt[:9] + df['Date'].astype(str)
        # df['Date']='20210701T'+df['Date'].str.replace(',','-')
        print('Success')
        df.to_csv(f'outputs/{dt}.csv', index=False)
        time.sleep(5)


if __name__ == '__main__':
    # load_dotenv()
    # air_lst = ['SFO', 'SJC', 'OAK', 'LAX']
    # get_node = os.getenv('NODE_BOOL')
    # get_DAM = os.getenv('DAM_BOOL')
    # get_CO2 = os.getenv('CO2_BOOL')
    # get_all = os.getenv('ALL_BOOL')
    # Num = int(os.getenv('NUM_OF_DAYS'))
    # start_date=os.getenv('START_DATE')
    # node_name = os.getenv('NODE_NAME')
    node_name = '0096WD_7_N001'
    # get_node_DAM(node_name, get_date('2022-08-24', Num), 'outputs')
    df1 = get_prices(node_name, '20220824T09:00-0000', '20220824T23:55-0000')
    df = get_dam(node_name, '20220824T09:00-0000', '20220824T23:55-0000')
    df.to_csv('tst.csv')
    df1.to_csv('tst2.csv')

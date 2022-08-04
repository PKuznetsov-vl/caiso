import glob
import io
import os
import time
import zipfile
from dateutil import rrule
from datetime import datetime, timedelta, date
import requests
from pycaiso.oasis import Node
import pandas as pd


def get_date(_from):
    now = date.fromisoformat(_from)
    end_date = now + timedelta(days=365)

    listofdays = rrule.rrule(rrule.DAILY, dtstart=now, until=end_date)
    listofdays = list(map(str, listofdays))
    listofdays = list(
        map(lambda x: x.replace('-', '').replace(' ', 'T').rstrip(listofdays[0][14:]) + '00:00-0000', listofdays))
    print(listofdays)
    return listofdays


def concate_all(path):
    all_files = glob.glob(os.path.join(path, "*.csv"))

    li = []
    print(all_files)
    for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0, low_memory=False)
        li.append(df)

    frame = pd.concat(li, axis=0, ignore_index=True)
    frame.to_csv('multi.csv')


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


def my_req_one(name, dates):
    # df1 = get_prices(name, '20210101T00:00-0000', '20210115T00:00-0000')
    # time.sleep(6)
    # df2 = get_prices(name, '20210116T00:00-0000', '20221231T00:00-0000')
    # time.sleep(6)
    # df3 = get_prices(name, '20210201T00:00-0000', '20210215T00:00-0000')
    # time.sleep(6)
    # df4 = get_prices(name, '202102016T00:00-0000', '20210228T00:00-0000')
    # time.sleep(6)
    # df5 = get_prices(name, '20210301T00:00-0000', '20210315T00:00-0000')
    # time.sleep(6)
    # df6 = get_prices(name, '20210316T00:00-0000', '20210331T00:00-0000')
    # time.sleep(6)
    # df7 = get_prices(name, '20210401T00:00-0000', '20210415T00:00-0000')
    # time.sleep(6)
    # df8 = get_prices(name, '20210416T00:00-0000', '20210430T00:00-0000')
    # time.sleep(6)
    # df9 = get_prices(name, '20210501T00:00-0000', '20210515T00:00-0000')
    # time.sleep(6)
    # df10 = get_prices(name, '20210516T00:00-0000', '20210531T00:00-0000')
    # time.sleep(6)
    # df11 = get_prices(name, '20210616T00:00-0000', '20210615T00:00-0000')
    # time.sleep(6)
    # df12= get_prices(name, '20210616T00:00-0000', '20210630T00:00-0000')
    # time.sleep(6)
    # df13 = get_prices(name, '20210701T00:00-0000', '20210715T00:00-0000')
    # time.sleep(6)
    # df14 = get_prices(name, '20210716T00:00-0000', '20220131T00:00-0000')
    # time.sleep(6)
    # df15 = get_prices(name, '20210801T00:00-0000', '20210815T00:00-0000')
    # time.sleep(6)
    # df16 = get_prices(name, '202108016T00:00-0000', '20210831T00:00-0000')
    # time.sleep(6)
    # df17 = get_prices(name, '20210901T00:00-0000', '20210915T00:00-0000')
    # time.sleep(6)
    # df18 = get_prices(name, '202109016T00:00-0000', '20210930T00:00-0000')
    # time.sleep(6)
    # df19 = get_prices(name, '20211001T00:00-0000', '20211015T00:00-0000')
    # time.sleep(6)
    # df20 = get_prices(name, '20211016T00:00-0000', '20211031T00:00-0000')
    # time.sleep(6)
    # df21 = get_prices(name, '20211101T00:00-0000', '20211115T00:00-0000')
    # time.sleep(6)
    # df22 = get_prices(name, '20211116T00:00-0000', '20211130T00:00-0000')
    # time.sleep(6)
    # df23 = get_prices(name, '20211201T00:00-0000', '20211215T00:00-0000')
    # time.sleep(6)
    # df24 = get_prices(name, '20211216T00:00-0000', '2021231T00:00-0000')
    # time.sleep(6)
    # df25 = get_prices(name, '20220101T00:00-0000', '20220115T00:00-0000')
    # time.sleep(6)
    # df26 = get_prices(name, '20220116T00:00-0000', '20220131T00:00-0000')
    # time.sleep(6)
    list_of_csv = []
    for day in range(len(dates) - 1):
        time.sleep(6)
        df = get_prices(name, dates[day], dates[day + 1])
        print(df.head())
        list_of_csv.append(df)
    print(len(list_of_csv))
    # df27 = get_prices(name, '20220201T00:00-0000', '20220215T00:00-0000')
    # time.sleep(6)
    # df28 = get_prices(name, '20220216T00:00-0000', '20220228T00:00-0000')
    # time.sleep(6)
    # df29 = get_prices(name, '20220301T00:00-0000', '20220315T00:00-0000')
    # time.sleep(6)
    # df30 = get_prices(name, '20220316T00:00-0000', '20220331T00:00-0000')
    # time.sleep(6)
    # df31 = get_prices(name, '20220401T00:00-0000', '20220415T00:00-0000')
    # time.sleep(6)
    # df32 = get_prices(name, '20220416T00:00-0000', '20220430T00:00-0000')
    # time.sleep(6)
    # df33 = get_prices(name, '20220501T00:00-0000', '20220515T00:00-0000')
    # time.sleep(6)
    # df34 = get_prices(name, '20220515T00:00-0000', '20220531T00:00-0000')
    # time.sleep(6)
    # df35 = get_prices(name, '20220601T00:00-0000', '20220615T00:00-0000')
    # time.sleep(6)
    # df36 = get_prices(name, '20220616T00:00-0000', '20220630T00:00-0000')
    # time.sleep(6)
    # df37 = get_prices(name, '20220701T00:00-0000', '20220715T00:00-0000')
    # time.sleep(6)
    #
    # df_f = pd.concat([#df13, df14, df15, df16, df17, df18, df19, df20, df21, df22, df23, df24, df25, df26,
    #                  df27, df28,
    #                   df29, df30, df31, df32, df33, df34, df35, df36,
    #                   df37], ignore_index=True)
    # print('Success')
    # df_f.to_csv(f'csv/{name}.csv', index=False)


def my_req_int():
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


def my_req_DAM(name):
    # df1 = get_prices(name, '20210101T00:00-0000', '20210115T00:00-0000')
    # time.sleep(6)
    # df2 = get_prices(name, '20210116T00:00-0000', '20221231T00:00-0000')
    # time.sleep(6)
    # df3 = get_prices(name, '20210201T00:00-0000', '20210215T00:00-0000')
    # time.sleep(6)
    # df4 = get_prices(name, '202102016T00:00-0000', '20210228T00:00-0000')
    # time.sleep(6)
    # df5 = get_prices(name, '20210301T00:00-0000', '20210315T00:00-0000')
    # time.sleep(6)
    # df6 = get_prices(name, '20210316T00:00-0000', '20210331T00:00-0000')
    # time.sleep(6)
    # df7 = get_prices(name, '20210401T00:00-0000', '20210415T00:00-0000')
    # time.sleep(6)
    # df8 = get_prices(name, '20210416T00:00-0000', '20210430T00:00-0000')
    # time.sleep(6)
    # df9 = get_prices(name, '20210501T00:00-0000', '20210515T00:00-0000')
    # time.sleep(6)
    # df10 = get_prices(name, '20210516T00:00-0000', '20210531T00:00-0000')
    # time.sleep(6)
    # df11 = get_prices(name, '20210616T00:00-0000', '20210615T00:00-0000')
    # time.sleep(6)
    # df12= get_prices(name, '20210616T00:00-0000', '20210630T00:00-0000')
    # time.sleep(6)
    df13 = get_dam(name, '20210701T00:00-0000', '20210715T00:00-0000')
    time.sleep(6)
    df14 = get_dam(name, '20210716T00:00-0000', '20210731T00:00-0000')
    time.sleep(6)
    df15 = get_dam(name, '20210801T00:00-0000', '20210815T00:00-0000')
    time.sleep(6)
    df16 = get_dam(name, '202108016T00:00-0000', '20210831T00:00-0000')
    time.sleep(6)
    df17 = get_dam(name, '20210901T00:00-0000', '20210915T00:00-0000')
    time.sleep(6)
    df18 = get_dam(name, '202109016T00:00-0000', '20210930T00:00-0000')
    time.sleep(6)
    df19 = get_dam(name, '20211001T00:00-0000', '20211015T00:00-0000')
    time.sleep(6)
    df20 = get_dam(name, '20211016T00:00-0000', '20211031T00:00-0000')
    time.sleep(6)
    df21 = get_dam(name, '20211101T00:00-0000', '20211115T00:00-0000')
    time.sleep(6)
    df22 = get_dam(name, '20211116T00:00-0000', '20211130T00:00-0000')
    time.sleep(6)
    df23 = get_dam(name, '20211201T00:00-0000', '20211215T00:00-0000')
    time.sleep(6)
    df24 = get_dam(name, '20211216T00:00-0000', '20211231T00:00-0000')
    time.sleep(6)
    df25 = get_dam(name, '20220101T00:00-0000', '20220115T00:00-0000')
    time.sleep(6)
    df26 = get_dam(name, '20220116T00:00-0000', '20220131T00:00-0000')
    time.sleep(6)
    df27 = get_dam(name, '20220201T00:00-0000', '20220215T00:00-0000')
    time.sleep(6)
    df28 = get_dam(name, '20220216T00:00-0000', '20220228T00:00-0000')
    time.sleep(6)
    df29 = get_dam(name, '20220301T00:00-0000', '20220315T00:00-0000')
    time.sleep(6)
    df30 = get_dam(name, '20220316T00:00-0000', '20220331T00:00-0000')
    time.sleep(6)
    df31 = get_dam(name, '20220401T00:00-0000', '20220415T00:00-0000')
    time.sleep(6)
    df32 = get_dam(name, '20220416T00:00-0000', '20220430T00:00-0000')
    time.sleep(6)
    df33 = get_dam(name, '20220501T00:00-0000', '20220515T00:00-0000')
    time.sleep(6)
    df34 = get_dam(name, '20220515T00:00-0000', '20220531T00:00-0000')
    time.sleep(6)
    df35 = get_dam(name, '20220601T00:00-0000', '20220615T00:00-0000')
    time.sleep(6)
    df36 = get_dam(name, '20220616T00:00-0000', '20220630T00:00-0000')
    time.sleep(6)
    df37 = get_dam(name, '20220701T00:00-0000', '20220715T00:00-0000')
    time.sleep(6)
    df38 = get_dam(name, '20220716T00:00-0000', '20220731T00:00-0000')
    df_f = pd.concat([df13, df14, df15, df16, df17, df18, df19, df20, df21, df22, df23, df24, df25, df26,
                      df27, df28, df29, df30, df31, df32, df33, df34, df35, df36, df37, df38], ignore_index=True)
    print('Success')
    df_f.to_csv(f'csv/{name}_DAM.csv', index=False)


if __name__ == '__main__':
    my_req_one('HOLLISTR_1_N101', get_date('2021-08-05'))
    # my_req_DAM('HOLLISTR_1_N101')
    # name='GRDNWEST_1_N001'
    # # df13 = get_dam(name, '20210701T00:00-0000', '20210715T00:00-0000')
    # df13 = get_prices(name, '20220802T04:00-0000', '20220802T05:00-0000')
    # print(df13.head(100))
    # df13.to_csv('tst.csv')
    # my_req_int()
    # path = '/Users/pavel/PycharmProjects/caiso/csv'
    # concate_all(path)

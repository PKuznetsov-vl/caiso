import io
import time
import zipfile

import requests
from pycaiso.oasis import Node
from datetime import datetime
import pandas as pd


# cj = Node(names_lst[0])
# cj_lmps = cj.get_lmps(datetime(2022, 1, 1), datetime(2022, 1, 30))
# cj_lmps = cj.get_lmps(datetime(2021, 1, 1), datetime(2021, 1, 15))
# cj_lmps.to_csv('tst.csv')
# print(cj_lmps.head())
# cj = Node(name)
# select pnode
def ds(names_lst):
    for name in names_lst[3]:
        cj = Node(name)
        # create dataframe with LMPS from arbitrary period (30 day maximum).
        # time.sleep(10)
        cj_lmps = cj.get_lmps(datetime(2022, 6, 1), datetime(2022, 6, 2))
        # time.sleep(10)
        # cj_lmps2=cj.get_lmps(datetime(2022, 1, 15), datetime(2022, 1,29 ))
        # time.sleep(10)
        cj_lmps.to_csv('tst.csv')
        # cj_lmps3 = cj.get_lmps(datetime(2022, 1, 30), datetime(2022, 2, 14))
        # time.sleep(10)
        # cj_lmps4 = cj.get_lmps(datetime(2022, 2, 14), datetime(2022, 3, 1))
        # time.sleep(10)
        # cj_lmps5 = cj.get_lmps(datetime(2022, 3, 1), datetime(2022, 3, 15))
        # time.sleep(10)
        # cj_lmps6 = cj.get_lmps(datetime(2022, 3, 15), datetime(2022, 3, 30))
        # df_f=pd.concat([cj_lmps, cj_lmps2], ignore_index=True)
        # df_f.to_csv(f'outputs/{name}.csv')


#     cj_lmps2 = cj.get_lmps(datetime(2022, 1, 31), datetime(2022, 2, 28))

# ds()
def get_prices(nodename, startdate, enddate):
    rsp = requests.get(
        f'http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_INTVL_LMP&startdatetime={startdate}&enddatetime={enddate}&version=1&market_run_id=RTM&node={nodename}&resultformat=6',
        timeout=135)

    z = zipfile.ZipFile(io.BytesIO(rsp.content))
    csv = z.open(z.namelist()[0])
    df = pd.read_csv(csv)
    return df


def my_req():
    rsp = requests.get(
        'http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_LMP&startdatetime=20220729T15:00-0000&enddatetime=20220729T23:00-0000&version=1&market_run_id=DAM&node=MALCHA_7_B1&resultformat=6')
    z = zipfile.ZipFile(io.BytesIO(rsp.content))
    z.extractall("csv")


# my_req()

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
        df_f=pd.concat([df1,df2,df3,df4,df5,df6,df7,df8],ignore_index=True)
        print('Success')
        df_f.to_csv(f'csv/{name}.csv',index=False)


my_req_int()
# sp15 = Node.SP15()
# sp15_lmps = sp15.get_lmps(datetime(2021, 1, 1), datetime(2021, 1, 2))
# sp15_lmps.to_csv('tst.csv')

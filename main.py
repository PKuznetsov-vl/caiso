import io
import time
import zipfile

import requests
from pycaiso.oasis import Node
from datetime import datetime
import pandas as pd

df = pd.read_csv('LMPLocations.csv')
names_lst = df['name'].tolist()
print(names_lst[0])
# cj = Node(names_lst[0])
# cj_lmps = cj.get_lmps(datetime(2022, 1, 1), datetime(2022, 1, 30))
# cj_lmps = cj.get_lmps(datetime(2021, 1, 1), datetime(2021, 1, 15))
# cj_lmps.to_csv('tst.csv')
#print(cj_lmps.head())
# cj = Node(name)
# select pnode
def ds():
    for name in names_lst[3]:
        cj = Node(name)
        # create dataframe with LMPS from arbitrary period (30 day maximum).
        # time.sleep(10)
        cj_lmps = cj.get_lmps(datetime(2022, 6, 1), datetime(2022 , 6, 2))
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

#ds()
def my_req():
    rsp=requests.get('http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_LMP&startdatetime=20220729T15:00-0000&enddatetime=20220729T23:00-0000&version=1&market_run_id=DAM&node=MALCHA_7_B1&resultformat=6')
    z = zipfile.ZipFile(io.BytesIO(rsp.content))
    z.extractall("csv")
my_req()

def my_req_int():
    rsp=requests.get('http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_LMP&startdatetime=20220729T15:00-0000&enddatetime=20220729T23:00-0000&version=1&market_run_id=DAM&node=MALCHA_7_B1&resultformat=6')
    z = zipfile.ZipFile(io.BytesIO(rsp.content))
    z.extractall("csv")
my_req_int()
# sp15 = Node.SP15()
# sp15_lmps = sp15.get_lmps(datetime(2021, 1, 1), datetime(2021, 1, 2))
# sp15_lmps.to_csv('tst.csv')

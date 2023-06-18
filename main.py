from caisoAPI import Caiso
import datetime
import pandas as pd 


def main():
    caiso = Caiso()
    caiso.start_session_refresh() # optional
    folder_path = 'tmpdata'
    df = pd.read_csv('LMPLocations.csv')
    nodenames = df['name'].tolist()
    startdate = datetime.datetime(2022, 4, 30, 9, 0)
    enddate = datetime.datetime(2022, 5, 7, 0, 0)

    for nodename in nodenames:
        caiso.nodename = nodename

        df_prices = caiso.get_prices(startdate, enddate)
        if df_prices is not None:
            file_path = f'{folder_path}/{nodename}-RTM-{startdate.strftime("%Y%m%d")}-{enddate.strftime("%Y%m%d")}.csv'
            with open(file_path, 'w', newline='') as file:
                df_prices.to_csv(file, index=False)


        dam = caiso.get_dam(startdate, enddate)
        if dam is not None:
            file_path = f'{folder_path}/{nodename}-DAM-{startdate.strftime("%Y%m%d")}-{enddate.strftime("%Y%m%d")}.csv'
            with open(file_path, 'w', newline='') as file:
                df_prices.to_csv(file, index=False)
            
if __name__ == "__main__":
    main()

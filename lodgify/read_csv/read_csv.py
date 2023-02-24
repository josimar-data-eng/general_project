import numpy as np
import pandas as pd
import datetime as dt
from IPython.display import display
from pandas.tseries.offsets import DateOffset

def read_bookings_csv():

    # Reading csv and getting only the coluns needed and trustable
    df = pd.read_csv("input_files/bookings.csv")[['subscriber_id', 'booking_status','booking_date']]
    
    # Creating datetime columns to know the loading time
    df['datetime_load'] = dt.datetime.now()

    # filter to making tests
    df = df[(df.subscriber_id == 156)]
    
    # Creating primary key
    df = df.reset_index()
    df = df.rename(columns={"index":"id"})
    df['id'] = df.index + 0
    
    display(df.to_string())
    # return df



def read_subscription_csv():

    #variables
    lst = []

    #reading csv and selecting only the necessary and trust columns
    df = pd.read_csv("input_files/subscription.csv")[['sub_id','status','dates']]
    
    #filter
    # df = df.loc[df['sub_id'].isin([156,256])]

    #creating datetime columns to know the loading time
    df['datetime_load'] = dt.datetime.now()

    # Adjusting dates field to be able to parse it to datetime to make sure we can fill the month gaps correctly
    df['dates'] = df['dates'].replace('-','',regex=True)
    df['formated_dates'] = df.apply(lambda x: x['dates'][:]+'01', axis=1)
    df['formated_dates'] = pd.to_datetime(df['formated_dates'],format='%Y%m%d')

    # sorting to create next_date, previous_date, and previous_status columns correctly
    df.sort_values(by = ['sub_id', 'formated_dates'], ascending = [True, True],inplace=True)

    # Lead Function
    df['next_date'] = df['formated_dates'].shift(-1).fillna('')
    # Lag Function
    df['previous_date'] = df['formated_dates'].shift(1).fillna('')
    # Creating a lag function to get the previous status to assign it to the right rows after (get the status from the previous existing/single month)
    df['previous_status'] = df['status'].shift(1).fillna('')

    #diff dates to get month difference
    df['diff_months'] = (((df['next_date'] - df['formated_dates'])/np.timedelta64(1,'M')).fillna(0)).round(decimals = 0).astype(int)



    for idx,row in df.iterrows():
        if row['formated_dates'] == row['previous_date'] or row['formated_dates'] == row['next_date']:
            df.at[idx,'is_duplicate'] = True
        else:
            df.at[idx,'is_duplicate'] = False

    df_duplicates = df[df["is_duplicate"] == True]     # option2: df2=df.query("is_duplicate == True")
    # df_no_duplicates = df[df["is_duplicate"] == False]

    # Deduplicating
    for idx, row in df_duplicates.iterrows():
        if (row['next_date'] == row['formated_dates']):
            df_duplicates.drop(idx,inplace=True)            # WARNING :A value is trying to be set on a copy of a slice from a DataFrame
        else:
            df_duplicates.at[idx,'status'] = 'nan'


    union = pd.concat([df_duplicates,df.query("is_duplicate == False")])
    union.sort_values(by = ['sub_id', 'formated_dates'], ascending = [True, True],inplace=True)

    # Adding new rows to fill the month gaps and assigning the status from a previous existing/single month
    for index, row in union.iterrows():
        if (row['diff_months']) > 1:
            _index = row['diff_months']
            for i in range(1,_index):
                new_row = pd.DataFrame({  'sub_id':[row['sub_id']]
                                        , 'status':[row['status']]
                                        , 'formated_dates': [row['formated_dates']+pd.DateOffset(months=1)]
                                        , 'datetime_load':[dt.datetime.now()]
                                    })
                row['formated_dates'] += pd.DateOffset(months=1)
                union = pd.concat([union, new_row])
    union.sort_values(by = ['sub_id', 'formated_dates'], ascending = [True, True],inplace=True)                


    union.to_csv("union.csv")
    df_union = pd.read_csv("union.csv")

    for idx,row in df_union.iterrows():
        if row['is_duplicate'] == True:
            df_union.loc[idx,   'status'] = df_union.loc[idx-1,   'status']


    # Cleaning the columns that was used to help the iterations.
    df_union['formated_date'] = df_union.apply(lambda x: str(x['formated_dates'])[:7], axis=1)
    df_union.drop(labels=['dates','formated_dates','next_date','previous_date','previous_status','diff_months','is_duplicate'], axis=1, inplace=True)
    df_union.rename(columns={'formated_date' :'dates'},inplace=True)

    # Creating primary key
    df_union = df_union.reset_index()
    df_union = df_union.rename(columns={"index":"id"})
    df_union['id'] = df_union.index + 0

    print("========================================================")
    df_union = df_union[['id','sub_id', 'status', 'datetime_load', 'dates']]
    display(df_union.to_string())
    df_union.to_csv("final_table")
    print("========================================================")


    # # Iterating through dataframe rows to find the duplicate (same year-month) to then delete it
    # for index, row in df.iterrows():
    #     if (row['next_date'] == row['formated_dates']):
    #         key = str(row['sub_id'])
    #         date = str(row['dates'])
    #         previous_status = row['previous_status']
    #         lst.append(key+"-"+date+"-"+previous_status) # List with the values that will be used to define what row to select to assign the previous status
    #         df.drop(index,inplace=True)

    # # # Assigning the status from a previous existing/single month
    # for index, row in df.iterrows():
    #     for i in lst:
    #         if (str(row['sub_id'])+"-"+str(row['dates'])) == i:
    #             df.at[index,'status']=row['previous_status']

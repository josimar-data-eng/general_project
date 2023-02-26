import csv
import numpy as np
import pandas as pd
import datetime as dt
import pandasql as ps
from pandasql import sqldf
# from datetime import datetime
from IPython.display import display
from pandas.tseries.offsets import DateOffset



def read_bookings_csv():

    # Reading csv and getting only the coluns needed and trustable
    df = pd.read_csv("files/raw/booking.csv")[['subscriber_id', 'booking_status','booking_date']]
    
    # filter to making tests
    df = df.loc[df['subscriber_id'].isin([156,256])]
    
    # Creating datetime columns to know the loading time
    df['datetime_load'] = dt.datetime.now()

    for idx,row in df.iterrows():
        date = str(row['booking_date'])[:10]
        month = str(row['booking_date'])[5:7]
        year_month = (str(row['booking_date']))[:7]          
        df.at[idx,'month'] = month
        df.at[idx,'date'] = date
        df.at[idx,'booking_year_month']= year_month
    
    # Creating primary key
    df = df.reset_index()
    df = df.rename(columns={"index":"id"})
    df['id'] = df.index + 0

    df.to_csv("files/stg/booking.csv")

def read_subscription_csv():

    #reading csv and selecting only the necessary and trust columns
    df = pd.read_csv("files/raw/subscription.csv")[['sub_id','status','dates']]
    
    #filter
    df = df.loc[df['sub_id'].isin([156,256])]

    #creating datetime columns to know the loading time
    df['datetime_load'] = dt.datetime.now()
    # Adjusting dates field to be able to parse it to datetime to make sure we can fill the month gaps correctly
    df['dates'] = df['dates'].replace('-','',regex=True)
    df['formated_dates'] = df.apply(lambda x: x['dates'][:]+'01', axis=1)
    df['formated_dates'] = pd.to_datetime(df['formated_dates'],format='%Y%m%d')

    # sorting to create next_date, previous_date, and previous_status columns correctly
    df.sort_values(by = ['sub_id', 'formated_dates'], ascending = [True, True],inplace=True)


    # Lead and Lag Functions to create the months's difference between the records to check the fill gap month needed
    df['next_date'] = df['formated_dates'].shift(-1).fillna('')
    df['previous_date'] = df['formated_dates'].shift(1).fillna('')
    #diff dates to get month difference to be able to fill the gap months
    df['diff_months'] = (((df['next_date'] - df['formated_dates'])/np.timedelta64(1,'M')).fillna(0)).round(decimals = 0).astype(int)


    # Make the separation between dataframes, one with duplicates and another with no duplicates,
    # to cleand the duplicate dataframe, and set null for status to use it as a criteria to fill getting the previous month status

    # For loop to flag if the month-year of the current record exist in another record
    for idx,row in df.iterrows():
        if row['formated_dates'] == row['previous_date'] or row['formated_dates'] == row['next_date']:
            df.at[idx,'is_duplicate'] = True
        else:
            df.at[idx,'is_duplicate'] = False

    # Dataframe with duplicates records
    df_duplicates = df[df["is_duplicate"] == True]   #option2: df2=df.query("is_duplicate == True")

    # Deduplicating
    for idx, row in df_duplicates.iterrows():
        if (row['next_date'] == row['formated_dates']):
            df_duplicates.drop(idx,inplace=True)            # WARNING :A value is trying to be set on a copy of a slice from a DataFrame
        else:
            df_duplicates.at[idx,'status'] = 'nan'

    # Dataframes concatenation
    stg_subscription_df = pd.concat([df_duplicates,df.query("is_duplicate == False")])
    stg_subscription_df.to_csv("files/stg/subscription_concatened.csv")
    stg_subscription_df = pd.read_csv("files/stg/subscription_concatened.csv")

    # Filling the status column getting from a previous existing/single month
    stg_subscription_df.sort_values(by = ['sub_id', 'formated_dates'], ascending = [True, True],inplace=True)    
    for idx,row in stg_subscription_df.iterrows():
        if row['is_duplicate'] == True:
            stg_subscription_df.loc[idx,'status'] = stg_subscription_df.loc[idx-1,'status']


    # Adding new rows to fill the month gaps and assigning the status from a previous existing/single month
        # For this step, I'm not sure if that's the case asked, or if it was only my understand.
        # btw, if it was only my understand it's only comment this step, otherwise uncomment
    
    stg_subscription_df['formated_dates'] = pd.to_datetime(stg_subscription_df['formated_dates'],format='%Y-%m-%d')
    for index, row in stg_subscription_df.iterrows():
        if (row['diff_months']) > 1:
            _index = row['diff_months']
            for i in range(1,_index):
                new_row = pd.DataFrame({  'sub_id':[row['sub_id']]
                                        , 'status':[row['status']]
                                        , 'formated_dates': [row['formated_dates']+pd.DateOffset(months=1)]
                                        , 'datetime_load':[dt.datetime.now()]
                                    })
                row['formated_dates'] += pd.DateOffset(months=1)
                stg_subscription_df = pd.concat([stg_subscription_df, new_row])
    

    # # Cleaning the columns that was used to help the iterations.
    stg_subscription_df['formated_date'] = stg_subscription_df.apply(lambda x: str(x['formated_dates'])[:7], axis=1)
    stg_subscription_df.drop(labels=['dates','formated_dates','next_date','previous_date','diff_months','is_duplicate'], axis=1, inplace=True)
    stg_subscription_df.rename(columns={'formated_date' :'sub_year_month'},inplace=True)

    # # Creating primary key
    stg_subscription_df = stg_subscription_df.reset_index()
    stg_subscription_df = stg_subscription_df.rename(columns={"index":"id"})
    stg_subscription_df['id'] = stg_subscription_df.index + 0

    stg_subscription_df = stg_subscription_df[['id','sub_id', 'status', 'datetime_load', 'sub_year_month']]
    stg_subscription_df.to_csv("files/stg/subscription.csv")
    display(stg_subscription_df)    

def sql_query():

    df = pd.read_csv("final_table.csv")
    df_result = (duckdb.query("select max(sub_id) max_sub_id from df").df())
    new_var = int(df_result['max_sub_id'])

    print(type(new_var))





def stage_layer_manipulation():

    #----------------------#
    #--------BOOKING-------#
    #----------------------#

    # Getting the first and last subscription date per each id/month
    columns = ['subscriber_id','booking_status','booking_date','booking_year_month','month','date']
    stg_booking_df = pd.read_csv("files/stg/booking.csv")[columns]

    stg_booking_df_filter = stg_booking_df.query("booking_status not in ('Canceled','')")  # option 2 -> df_duplicates = df[df["is_duplicate"] == True]
    first_subscription_df = stg_booking_df_filter.groupby(['subscriber_id']).agg(first_subscription_date=('date', 'min')) #  {'date': ['min']})
    first_subscription_df.to_csv("files/stg/first_subscription_per_id_month.csv")    


    # Do I need to use filter in that case as well because the subscription need to be effective only if the status is not canceled?
    stg_booking_df_last = stg_booking_df.groupby(['subscriber_id','booking_year_month']).agg(last_booking_date_per_month=('date', 'max'))
    stg_booking_df_last.to_csv("files/stg/last_booking_per_id_month.csv")



    #----------------------#
    #-----SUBSCRIPTION-----#
    #----------------------#

    stg_subscription_df = pd.read_csv("files/stg/subscription.csv")
    stg_subscription_df['sub_date'] = stg_subscription_df['sub_year_month']+"-01"


    #--------------------------------------------------------------------#
    #- How many months has passed since their first subscription month? -#
    #--------------------------------------------------------------------#

    # According to the booking table
    months_since_first_sub_df = stg_booking_df_last.merge(first_subscription_df, on='subscriber_id', how='left')
    months_since_first_sub_df['formated_first_date'] = pd.to_datetime(months_since_first_sub_df['first_subscription_date'    ],format='%Y-%m-%d')
    months_since_first_sub_df['formated_last_date' ] = pd.to_datetime(months_since_first_sub_df['last_booking_date_per_month'],format='%Y-%m-%d')
    months_since_first_sub_df['booking_year_month'] = months_since_first_sub_df.apply(lambda x: str(x['last_booking_date_per_month'])[:7], axis=1)
    months_since_first_sub_df['months_since_first_sub'] = (((months_since_first_sub_df['formated_last_date'] - months_since_first_sub_df['formated_first_date'])/np.timedelta64(1,'M')).fillna(0)).astype(int)
    months_since_first_sub_df.drop(labels=['formated_first_date','formated_last_date','last_booking_date_per_month','first_subscription_date'], axis=1, inplace=True)    
    months_since_first_sub_df.to_csv("files/sandbox/booking_months_since_first_sub.csv")


    # According to the subscription table
    # stg_subscription_renamed_df = stg_subscription_df
    stg_subscription_df.rename(columns={'sub_id':'subscriber_id'},inplace=True)
    months_since_first_sub_df = stg_subscription_df.merge(first_subscription_df, on='subscriber_id',how='left')
    months_since_first_sub_df['formated_sub_date'  ] = pd.to_datetime(months_since_first_sub_df['sub_date'],format='%Y-%m-%d')
    months_since_first_sub_df['formated_first_date'] = pd.to_datetime(months_since_first_sub_df['first_subscription_date'],format='%Y-%m-%d')
    months_since_first_sub_df['months_since_first_sub'] = (((months_since_first_sub_df['formated_sub_date'] - months_since_first_sub_df['formated_first_date'])/np.timedelta64(1,'M')).fillna(0)).round(decimals = 0).astype(int)
    months_since_first_sub_df = months_since_first_sub_df[['subscriber_id','sub_year_month','months_since_first_sub']]
    months_since_first_sub_df.to_csv("files/sandbox/subscribing_months_since_first_sub.csv")
    months_since_first_sub_df.sort_values(by = ['subscriber_id', 'sub_year_month'], ascending = [True, True],inplace=True)


    #------------------------------------------------------------#
    #- How many months they were an active/canceled subscriber? -#
    #------------------------------------------------------------#
    
    count_status_df = stg_subscription_df[['subscriber_id','status','sub_year_month']]

    for index, row in count_status_df.iterrows():
        if row['status'].lower().strip() == 'active':
            count_status_df.at[index,'is_active'] = 1
            count_status_df.at[index,'is_canceled'] = 0
        else:
            count_status_df.at[index,'is_active'] = 0
            count_status_df.at[index,'is_canceled'] = 1


    count_status_df['monhts_active'] = count_status_df['is_active'].cumsum().astype(int)
    count_status_df['monhts_canceled'] = count_status_df['is_canceled'].cumsum().astype(int)

    count_status_df = count_status_df[['subscriber_id', 'sub_year_month', 'monhts_active', 'monhts_canceled']]
    count_status_df.to_csv("files/sandbox/months_were_active_canceled.csv")

    # datetime.datetime.strptime(input,format)
    # datetime = row['last_booking_date_per_month'], '%Y/%m/%d')
    # print(datetime.date())


#   select  subscriber_id
#         , extract(date  from (min(booking_date))) first_subscription_date
#         , extract(month from (min(booking_date))) first_subscription_month
#   from raw_booking
#   where trim(booking_status) is not null or lower(trim(booking_status)) not in ('canceled','')
#   group by subscriber_id



# remove hierarquical index: df.columns = df.columns.droplevel(0)      


# with open("files/stg/booking.csv",'r') as in_file, open("files/stg/booking_out.csv",'w') as out_file:
        
#     csv_reader = csv.reader(in_file)            
#     csv_writer = csv.writer(out_file,lineterminator='\n')
    
#     rows = []
#     header = next(csv_reader)
#     header.append('booking_year_month')
#     rows.append(header)

#     for row in csv_reader:
#         booking_year_month = (row[4][:7])
#         row.append(booking_year_month)
#         rows.append(row)
#     csv_writer.writerows(rows)

# in_file.close()
# out_file.close()


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

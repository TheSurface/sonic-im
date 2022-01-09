import pandas as pd
import streamlit as st
import base64
from datetime import datetime, date
import math
from IPython.display import HTML
import json
from datetime_truncate import truncate
import pandasql as ps


st.set_page_config(page_title='Sonic IM File Processor',layout='wide')
sonic_im_client = st.sidebar.radio('Sonic IM Client',['Keeps','Ten Thousand','Cerebral', 'Justworks', 'Other'])

## FUNCTIONS ##
# Function to rebuild the budget with the Actual Drop Day and Next Drop Day columns
def rebuild_budget(daily_budget_df,date_series_name='Broadcast Week',show_series_name='Show Name'):

    rebuilt_budget_df = pd.DataFrame()

    for show in daily_budget_df[show_series_name].unique():
        temp_list = []
        temp_df = daily_budget_df[daily_budget_df[show_series_name] == show]
        drop_series = temp_df[date_series_name].reset_index()

        shifted_drop_series = drop_series.shift(-1)
        index_list = shifted_drop_series.index.values

        for item in index_list:
            if math.isnan(shifted_drop_series['index'][item]):
                temp_list.append(drop_series[date_series_name][item])
            else:
                temp_list.append(shifted_drop_series[date_series_name][item])

        temp_df.reset_index(inplace=True)
        temp_df['next_drop_date'] = pd.Series(temp_list)
        temp_df.drop(['index'],axis=1)

        rebuilt_budget_df = pd.concat([rebuilt_budget_df,temp_df],axis=0)

    return rebuilt_budget_df


def reduce_df(df,show_field_name,date_field_name,date_series_name='Broadcast Week'):
    
    crit_1 = df[date_series_name] <= cutoff_date
    crit_2 = (df[date_field_name] >= df[date_series_name]) & (df[date_field_name] < df['next_drop_date'])
    crit_3 = (df[date_series_name] == df['next_drop_date']) & (df[date_field_name] >= df[date_series_name])
    crit_4 = df[show_field_name].isnull()
    crit_5 = (df[date_field_name] >= df[date_series_name]) & (df[date_field_name] >= df['next_drop_date'])
    crit_6 = (df[date_field_name] <= df[date_series_name]) & (df[date_field_name] <= df['next_drop_date'])
    
    reduced_df = df[(crit_1 & (crit_2 | crit_3)) | (crit_1 & crit_4) | (crit_1 & (crit_5 | crit_6))]
    
    return reduced_df


def zero_out_crit(df,date_series_name='Broadcast Week'):
    crit_5 = (df['event_date'] > df[date_series_name]) & (df['event_date'] >= df['next_drop_date']) & (df[date_series_name] != df['next_drop_date'])
    crit_6 = (df['event_date'] < df[date_series_name]) & (df['event_date'] <= df['next_drop_date']) & (df[date_series_name] != df['next_drop_date'])
    crit_7 = (df[date_series_name] == df['next_drop_date']) & (df['event_date'] < df[date_series_name])

    return [crit_5,crit_6,crit_7]


if sonic_im_client == 'Keeps':
    st.header('Keeps File Processor')
    st.subheader('File Upload')
    st.write('1. Upload the following four files (Leads, Purchases, Keeps Daily Budget, Chartable Data) using the widgets below')
    st.write('2. Select a cutoff date')
    cutoff_date = st.date_input(label='',value=datetime.today().date())
    st.write('')
    st.write('')
    st.write('The output will be a file which can be downloaded and used to update the Keeps Tableau dashboard.')
    st.write('')
    st.write('')


    # Construct user interface
    col1, col2 = st.columns(2)

    with col1:
        uploaded_purchases = st.file_uploader(label='Purchases',accept_multiple_files=False)

    with col2:
        uploaded_leads = st.file_uploader(label='Leads',accept_multiple_files=False)

    uploaded_daily_budget = st.file_uploader(label='Keeps Budget',accept_multiple_files=False)
    uploaded_chartable_data = st.file_uploader(label='Chartable Data',accept_multiple_files=False)



    ### Create Data Frames ###
    if (uploaded_leads is not None) and (uploaded_daily_budget is not None) and (uploaded_purchases is not None) and (uploaded_chartable_data is not None):
        looker_file_purchases_df = pd.read_csv(uploaded_purchases,parse_dates=["User's First Non-refunded Purchase Date","Lead Created Date"])
        looker_file_leads_df = pd.read_csv(uploaded_leads,parse_dates=['Lead Created Date'])
        daily_budget_df = pd.read_csv(uploaded_daily_budget,parse_dates=['Broadcast Week','Actual Drop Day'])
        chartable_df = pd.read_csv(uploaded_chartable_data, parse_dates=['Date'])

        daily_budget_df['Client Rate'] = daily_budget_df['Client Rate'].apply(lambda x: str(x).replace('$','').replace(',','').replace(')','').replace('(','-'))
        daily_budget_df['Client Rate'] = daily_budget_df['Client Rate'].apply(lambda x: float(x))
        daily_budget_df['Broadcast Week'] = daily_budget_df['Broadcast Week'].apply(lambda x: x.date())

        df_budget = daily_budget_df
        df_leads = looker_file_leads_df
        df_purchases = looker_file_purchases_df


    ### VIEWS: Performance Summary, Chartable vs. Looker, Chartable-Looker Combined by Show ###
        # Create DataFrames from uploaded CSV files
        daily_budget_df = daily_budget_df.sort_values(by=['Show Name','Actual Drop Day'])


        # Create purchase and lead indicator column
        looker_file_purchases_df['orders'] = 1
        looker_file_leads_df['leads'] = 1


        # Aggregate purchase and lead data by date and show name
        looker_purchases_agg_df = looker_file_purchases_df.groupby(["User's First Non-refunded Purchase Date",'Utm Campaign']).sum()['orders'].reset_index()
        looker_leads_agg_df = looker_file_leads_df.groupby(['Lead Created Date','Utm Campaign']).sum()['leads'].reset_index()


        # Create columns for percent of show's audience that is male and female
        daily_budget_df['Percent Male'] = daily_budget_df['% M/F'].apply(lambda x: int(x.split('/')[0].strip('M'))/100)
        daily_budget_df['Percent Female'] = daily_budget_df['% M/F'].apply(lambda x: int(x.split('/')[1].strip('F'))/100)


        # Rebuild budget
        rebuilt_budget_df = rebuild_budget(daily_budget_df,'Actual Drop Day')


        # Define Looker Purchase and Looker Lead Pandas SQL
        # Looker Purchases #
        lp_code = '''
        
        SELECT
            a."Show Name",
            a."Host Name",
            a."Network",
            a."Chartable Tracking Y/N",
            a.Genre,
            a."Age Demo",
            a."Ad Type",
            a."Content Type",
            a."Test/Core",
            a.Placement,
            a.Format,
            a."Personally Endorsed",
            a."Downloads",
            a."Client Rate",
            DATE(a."Broadcast Week") AS "Broadcast Week",
            DATE(a."Actual Drop Day") AS "Actual Drop Day",
            a."Percent Male",
            a."Percent Female",
            DATE(a.next_drop_date) AS next_drop_date,
            SUM(CASE WHEN (b."User's First Non-refunded Purchase Date" >= a."Actual Drop Day" AND b."User's First Non-refunded Purchase Date" < a.next_drop_date) OR (a."Actual Drop Day" = a.next_drop_date AND b."User's First Non-refunded Purchase Date" >= a.next_drop_date) THEN b.orders ELSE 0 END) AS looker_orders
            
        FROM rebuilt_budget_df a
            LEFT JOIN looker_purchases_agg_df b ON a.UTM = b.'Utm Campaign'
            
        WHERE 
            (a."Broadcast Week" <= "{cutoff_date}" AND ((b."User's First Non-refunded Purchase Date" >= a."Actual Drop Day" AND b."User's First Non-refunded Purchase Date" < a.next_drop_date) OR
            (a."Actual Drop Day" = a.next_drop_date AND b."User's First Non-refunded Purchase Date" >= a.next_drop_date))) OR
            (a."Broadcast Week" <= "{cutoff_date}" AND b."User's First Non-refunded Purchase Date" IS NULL) OR
            (a."Broadcast Week" <= "{cutoff_date}" AND ((b."User's First Non-refunded Purchase Date" >= a."Actual Drop Day" AND b."User's First Non-refunded Purchase Date" >= a.next_drop_date) OR 
            (b."User's First Non-refunded Purchase Date" <= a."Actual Drop Day" AND b."User's First Non-refunded Purchase Date" <= a.next_drop_date)))
            
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
        '''.format(cutoff_date=cutoff_date)

        purchases_df = ps.sqldf(lp_code,locals())


        # Looker Leads #
        ll_code = '''
        SELECT
            a."Show Name",
            a."Host Name",
            a."Network",
            a."Chartable Tracking Y/N",
            a.Genre,
            a."Age Demo",
            a."Ad Type",
            a."Content Type",
            a."Test/Core",
            a.Placement,
            a.Format,
            a."Personally Endorsed",
            a."Downloads",
            a."Client Rate",
            DATE(a."Broadcast Week") AS "Broadcast Week",
            DATE(a."Actual Drop Day") AS "Actual Drop Day",
            a."Percent Male",
            a."Percent Female",
            DATE(a.next_drop_date) AS next_drop_date,
            SUM(CASE WHEN (b."Lead Created Date" >= a."Actual Drop Day" AND b."Lead Created Date" < a.next_drop_date) OR (a."Actual Drop Day" = a.next_drop_date AND b."Lead Created Date" >= a.next_drop_date) THEN b.leads ELSE 0 END) AS looker_leads
            
        FROM rebuilt_budget_df a
            LEFT JOIN looker_leads_agg_df b ON a.UTM = b.'Utm Campaign'
            
        WHERE 
            (a."Broadcast Week" <= "{cutoff_date}" AND ((b."Lead Created Date" >= a."Actual Drop Day" AND b."Lead Created Date" < a.next_drop_date) OR
            (a."Actual Drop Day" = a.next_drop_date AND b."Lead Created Date" >= a.next_drop_date))) OR
            (a."Broadcast Week" <= "{cutoff_date}" AND b."Lead Created Date" IS NULL) OR
            (a."Broadcast Week" <= "{cutoff_date}" AND ((b."Lead Created Date" >= a."Actual Drop Day" AND b."Lead Created Date" >= a.next_drop_date) OR 
            (b."Lead Created Date" <= a."Actual Drop Day" AND b."Lead Created Date" <= a.next_drop_date)))
            
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
        '''.format(cutoff_date=cutoff_date)

        leads_df = ps.sqldf(ll_code,locals())


        # Combine Looker Output Files
        looker_combined_df = pd.merge(purchases_df,leads_df,on=['Show Name','Host Name','Network','Chartable Tracking Y/N','Genre','Age Demo','Ad Type','Content Type','Test/Core','Placement','Format','Personally Endorsed','Downloads','Client Rate','Broadcast Week','Actual Drop Day','Percent Male','Percent Female','next_drop_date'],how='inner')


        # Aggregate Chartable Base File
        chartable_agg_df = chartable_df.groupby(['Date','Ad Campaign Name']).sum()[['Confirmed lead','Estimated lead','Confirmed purchase','Estimated purchase']].reset_index()


        # Define Chartable Pandas SQL
        # Chartable #
        chartable_code = '''
        SELECT
            a."Show Name",
            a."Host Name",
            a."Network",
            a."Chartable Tracking Y/N",
            a.Genre,
            a."Age Demo",
            a."Ad Type",
            a."Content Type",
            a."Test/Core",
            a.Placement,
            a.Format,
            a."Personally Endorsed",
            a."Downloads",
            a."Client Rate",
            DATE(a."Broadcast Week") AS "Broadcast Week",
            DATE(a."Actual Drop Day") AS "Actual Drop Day",
            a."Percent Male",
            a."Percent Female",
            DATE(a.next_drop_date) AS next_drop_date,
            SUM(CASE WHEN (b.Date >= a."Actual Drop Day" AND b.Date < a.next_drop_date) OR (a."Actual Drop Day" = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Estimated purchase" ELSE 0 END) AS chartable_estimated_purchases,
            SUM(CASE WHEN (b.Date >= a."Actual Drop Day" AND b.Date < a.next_drop_date) OR (a."Actual Drop Day" = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Confirmed purchase" ELSE 0 END) AS chartable_confirmed_purchases,
            SUM(CASE WHEN (b.Date >= a."Actual Drop Day" AND b.Date < a.next_drop_date) OR (a."Actual Drop Day" = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Estimated lead" ELSE 0 END) AS chartable_estimated_leads,
            SUM(CASE WHEN (b.Date >= a."Actual Drop Day" AND b.Date < a.next_drop_date) OR (a."Actual Drop Day" = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Confirmed lead" ELSE 0 END) AS chartable_confirmed_leads
            
        FROM rebuilt_budget_df a
            LEFT JOIN chartable_agg_df b ON a."Show Name" = b."Ad Campaign Name"
            
        WHERE 
            (a."Broadcast Week" <= "{cutoff_date}" AND ((b.Date >= a."Actual Drop Day" AND b.Date < a.next_drop_date) OR
            (a."Actual Drop Day" = a.next_drop_date AND b.Date >= a.next_drop_date))) OR
            (a."Broadcast Week" <= "{cutoff_date}" AND b.Date IS NULL) OR
            (a."Broadcast Week" <= "{cutoff_date}" AND ((b.Date >= a."Actual Drop Day" AND b.Date >= a.next_drop_date) OR 
            (b.Date <= a."Actual Drop Day" AND b.Date <= a.next_drop_date)))
            
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
        '''.format(cutoff_date=cutoff_date)

        chartable_total_df = ps.sqldf(chartable_code,locals())


        # Create the final output file
        output_df = pd.merge(looker_combined_df,chartable_total_df,on=['Show Name','Host Name','Network','Chartable Tracking Y/N','Genre','Age Demo','Ad Type','Content Type','Test/Core','Placement','Format','Personally Endorsed','Downloads','Client Rate','Broadcast Week','Actual Drop Day','Percent Male','Percent Female','next_drop_date'],how='inner')


    ### VIEWS: Monthly Calendar View ###
        df_budget['budget_spend_month'] = df_budget['Actual Drop Day'].apply(lambda x: truncate(x,'month'))
        df_budget_grouped = df_budget[df_budget['Broadcast Week'] <= cutoff_date].groupby(['Show Name','budget_spend_month','UTM']).sum()[['Client Rate']].reset_index()


        # Process leads dataframe
        df_leads['created_week'] = df_leads['Lead Created Date'].apply(lambda x: truncate(x,'week'))
        df_leads['created_month'] = df_leads['Lead Created Date'].apply(lambda x: truncate(x,'month'))
        df_leads.rename({'Lead Created Date':'event_date'},axis=1,inplace=True)


        # Process purchases dataframe
        df_purchases['created_week'] = df_purchases['User\'s First Non-refunded Purchase Date'].apply(lambda x: truncate(x,'week'))
        df_purchases['created_month'] = df_purchases['User\'s First Non-refunded Purchase Date'].apply(lambda x: truncate(x,'month'))
        df_purchases.rename({'User\'s First Non-refunded Purchase Date':'event_date'},axis=1,inplace=True)


        # Create calendar dataframe for cross join
        df_calendar = pd.Series(pd.date_range(start='2020-01-01',end='2022-12-31',freq='M')).reset_index()
        df_calendar.rename({0:'date'},inplace=True,axis=1)
        df_calendar.drop(labels='index',axis=1,inplace=True)
        df_calendar['date'] = df_calendar['date'].apply(lambda x: truncate(x,'month'))
        df_calendar['key'] = 1


        # Create unique UTM dataframe
        df_utms = pd.Series(df_budget['UTM'].unique()).reset_index()
        df_utms.rename({0:'UTM'},inplace=True,axis=1)
        df_utms.drop(labels='index',axis=1,inplace=True)
        df_utms['key'] = 1


        # Combine calendar and UTM dataframes
        df_base = pd.merge(df_calendar, df_utms, on ='key').drop("key", 1)

        
        # Combine base and budget dataframes
        df_base_budget = pd.merge(df_base, df_budget_grouped, how='left', left_on=['date','UTM'], right_on=['budget_spend_month','UTM'])
        df_base_budget['Client Rate'].fillna(0,inplace=True)


        # Create lead and purchase calendar dataframes
        df_leads_monthly_cal = pd.merge(df_base_budget, df_leads[['User ID','event_date','Utm Source','Utm Campaign','created_week','created_month']],how='left',left_on=['date','UTM'],right_on=['created_month','Utm Campaign'])
        df_leads_monthly_cal['type'] = 'Lead'

        df_purchases_monthly_cal = pd.merge(df_base_budget,df_purchases[['User ID','event_date','Utm Source','Utm Campaign','created_week','created_month']],how='left',left_on=['date','UTM'],right_on=['created_month','Utm Campaign'])
        df_purchases_monthly_cal['type'] = 'Purchase'


        # Create monthly output file
        df_output_monthly = pd.concat([df_leads_monthly_cal,df_purchases_monthly_cal])


        st.write('')
        st.write('')


        ### OUTPUT ###
        st.subheader('Data Source Output')
        st.write('')
        st.write('')

        # Create download link for Performance Summary, Looker vs. Chartable, and Chartable-Looker Combined views file
        output_csv = output_df.to_csv(index=False)
        b64 = base64.b64encode(output_csv.encode()).decode()  # some strings <-> bytes conversions necessary here
        output_href = f'<a href="data:file/csv;base64,{b64}" download="output.csv">Download your Output CSV File</a>'
        st.markdown(output_href, unsafe_allow_html=True)

        # Create download link for Performance Summary, Looker vs. Chartable, and Chartable-Looker Combined views file
        monthly_output_csv = df_output_monthly.to_csv(index=False)
        b64 = base64.b64encode(monthly_output_csv.encode()).decode()  # some strings <-> bytes conversions necessary here
        monthly_output_href = f'<a href="data:file/csv;base64,{b64}" download="monthly_output.csv">Download your Monthly Output CSV File</a>'
        st.markdown(monthly_output_href, unsafe_allow_html=True)

    
elif sonic_im_client == 'Ten Thousand':

    
    st.header('Sonic File Processor')
    st.subheader('File Upload')

    st.write('1. Upload the complete budget from Salesforce')
    uploaded_daily_budget = st.file_uploader(label='',accept_multiple_files=False)

    st.write('')
    st.write('')
    if uploaded_daily_budget is not None:
        st.write('2. Select the client you want to process')
        daily_budget_df = pd.read_csv(uploaded_daily_budget,parse_dates=['Date'])
        client = st.selectbox(label='',options=daily_budget_df['Account Name: Account Name'].unique())

        st.write('')
        st.write('')
        st.write("3. Upload the client's proprietary data and Chartable data")
        col1, col2 = st.columns(2)
        with col1:
            uploaded_client_data = st.file_uploader(label='Client Data',accept_multiple_files=False)

        with col2:
            uploaded_chartable_data = st.file_uploader(label='Chartable Data',accept_multiple_files=False)


        st.write('')
        st.write('')
        st.write('4. Select a cutoff date')
        cutoff_date = st.date_input(label='',value=datetime.today().date())

         ### Create Data Frames ###
        if uploaded_client_data is not None:
            tt_client_data_df = pd.read_csv(uploaded_client_data,parse_dates=['day'])

        elif uploaded_chartable_data is not None:
            chartable_df = pd.read_csv(uploaded_chartable_data, parse_dates=['Date'])

    else:
        pass




    ### Create CPO and CPL Extract ###
    if (uploaded_daily_budget is not None)  and (uploaded_client_data is not None):

        # Create DataFrames from uploaded CSV files
        daily_budget_df = daily_budget_df[daily_budget_df['Account Name: Account Name'] == client].sort_values(by=['Podcast/Station: Account Name','Date'])


        # Create columns for percent of show's audience that is male and female
        daily_budget_df['Percent Male'] = daily_budget_df['MF Split'].apply(lambda x: int(x.split('/')[0].split(' ')[1])/100)
        daily_budget_df['Percent Female'] = daily_budget_df['MF Split'].apply(lambda x: int(x.split('/')[1].split(' ')[2])/100)

        # Rebuild budget
        rebuilt_budget_df = rebuild_budget(daily_budget_df,date_series_name='Date',show_series_name='Podcast/Station: Account Name')


        # Create final DataFrames to export for Tableau data sources
        transactions_df = pd.merge(rebuilt_budget_df,tt_client_data_df,left_on=['Code'],right_on=['name'],how='left')


        # Change date column name to event_date
        transactions_df.rename({'day':'event_date'}, axis=1, inplace=True)


        # Convert date fields to just dates
        transactions_df['event_date'] = transactions_df['event_date'].apply(lambda x: x.date())
        transactions_df['Date'] = transactions_df['Date'].apply(lambda x: x.date())
        transactions_df['next_drop_date'] = transactions_df['next_drop_date'].apply(lambda x: x.date())


        # Create final leads and purchases DataFrames by eliminating negative date_diffs and including drops which had 0 leads or order
        transactions_df.loc[zero_out_crit(transactions_df,date_series_name='Date')[0] | zero_out_crit(transactions_df,date_series_name='Date')[1] | zero_out_crit(transactions_df,date_series_name='Date')[2],'orders'] = 0
        final_transactions_df = reduce_df(transactions_df,'name','event_date',date_series_name='Date')
        final_transactions_df.fillna(value={'orders':0},inplace=True)
        
        st.write('')
        st.write('')


        ### OUTPUT ###
        st.subheader('Data Source Output')
        st.write('')
        st.write('')

        # Create download link for transactions file
        client_data_csv = final_transactions_df.to_csv(index=False)
        st.download_button(label='Download Client Data',data=client_data_csv,file_name='client_data.csv',mime='text/csv')


    elif (uploaded_daily_budget is not None) and (uploaded_chartable_data is not None):


        ## Daily Budget Processing ##
        daily_budget_df = daily_budget_df[daily_budget_df['Account Name: Account Name'] == client].sort_values(by=['Podcast/Station: Account Name','Date'])
        daily_budget_df['Percent Male'] = daily_budget_df['MF Split'].apply(lambda x: int(x.split('/')[0].split(' ')[1])/100)
        daily_budget_df['Percent Female'] = daily_budget_df['MF Split'].apply(lambda x: int(x.split('/')[1].split(' ')[2])/100)


        # Rebuild budget
        rebuilt_budget_df = rebuild_budget(daily_budget_df,date_series_name='Date',show_series_name='Podcast/Station: Account Name')


        # Aggregate Chartable data
        chartable_agg_df = chartable_df.groupby(['Date','Ad Campaign Name']).sum().reset_index()


        # Define Chartable Pandas SQL
        chartable_code = '''
        SELECT
            a."Podcast/Station: Account Name",
            a."Host/Show",
            a."Network",
            a."Format",
            a.Code,
            a."MF Split",
            a.Age,
            a.Day,
            a."Content Type",
            a.Chartable,
            a."Placement Type",
            a.Placement,
            a.Product,
            a.Audience,
            a."Number of Slots",
            a."Gross Spot Rate",
            a."Gross CPM",
            a.Price,
            DATE(a.Date) AS "Date",
            a."Core/Test",
            a."Opportunity Name",
            a."Percent Male",
            a."Percent Female",
            DATE(a.next_drop_date) AS next_drop_date,
            SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b.Impressions ELSE 0 END) AS impressions,
            SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b.Reach ELSE 0 END) AS reach,
            SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Estimated Unique Visitors" ELSE 0 END) AS estimated_unique_visitors,
            SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Confirmed Unique Visitors" ELSE 0 END) AS confirmed_unique_visitors,
            SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Estimated purchase" ELSE 0 END) AS estimated_purchases,
            SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Confirmed purchase" ELSE 0 END) AS confirmed_purchases,
            SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Estimated Revenue" ELSE 0 END) AS estimated_revenue,
            SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Confirmed Revenue" ELSE 0 END) AS confirmed_revenue
            
        FROM rebuilt_budget_df a
            LEFT JOIN chartable_agg_df b ON a."Podcast/Station: Account Name" = b."Ad Campaign Name"
            
        WHERE 
            (a.Date <= "{cutoff_date}" AND ((b.Date >= a.Date AND b.Date < a.next_drop_date) OR
            (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date))) OR
            (a.Date <= "{cutoff_date}" AND b.Date IS NULL) OR
            (a.Date <= "{cutoff_date}" AND ((b.Date >= a.Date AND b.Date >= a.next_drop_date) OR 
            (b.Date <= a.Date AND b.Date <= a.next_drop_date)))
            
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
        '''.format(cutoff_date=cutoff_date)

        chartable_total_df = ps.sqldf(chartable_code,locals())


        st.write('')
        st.write('')



        ### OUTPUT ###
        st.subheader('Data Source Output')
        st.write('')
        st.write('')

        # Create download link for transactions file
        chartable_csv = chartable_total_df.to_csv(index=False)
        st.download_button(label='Download Chartable Data',data=chartable_csv,file_name='chartable.csv',mime='text/csv')


elif sonic_im_client == 'Cerebral':

    st.header('Sonic File Processor')
    st.subheader('File Upload')

    st.write('1. Upload the Cerebral Budget')
    uploaded_daily_budget = st.file_uploader(label='',accept_multiple_files=False)

    st.write('')
    st.write('')
    if uploaded_daily_budget is not None:
        st.write('2. Select the client you want to process')
        daily_budget_df = pd.read_csv(uploaded_daily_budget,parse_dates=['Date'])
        client = st.selectbox(label='',options=daily_budget_df['Account Name: Account Name'].unique())
        client_type = st.selectbox(label='',options=['Leads Only','Orders Only','All'])

        st.write('')
        st.write('')
        st.write("3. Upload the client's Chartable data")
        uploaded_chartable_data = st.file_uploader(label='Chartable Data',accept_multiple_files=False)


        st.write('')
        st.write('')
        st.write('4. Select a cutoff date')
        cutoff_date = st.date_input(label='',value=datetime.today().date())




    ### Create Data Frames ###
    if (uploaded_daily_budget is not None) and (uploaded_chartable_data is not None):
        daily_budget_df = pd.read_csv(uploaded_daily_budget,parse_dates=['Broadcast Week','Actual Drop Day'])
        chartable_df = pd.read_csv(uploaded_chartable_data, parse_dates=['Date'])

        daily_budget_df['Client Rate'] = daily_budget_df['Client Rate'].apply(lambda x: str(x).replace('$','').replace(',','').replace(')','').replace('(','-'))
        daily_budget_df['Client Rate'] = daily_budget_df['Client Rate'].apply(lambda x: float(x))
        daily_budget_df['Broadcast Week'] = daily_budget_df['Broadcast Week'].apply(lambda x: x.date())

        df_budget = daily_budget_df
        df_leads = looker_file_leads_df
        df_purchases = looker_file_purchases_df


    ### VIEWS: Performance Summary, Chartable vs. Looker, Chartable-Looker Combined by Show ###
        # Create DataFrames from uploaded CSV files
        daily_budget_df = daily_budget_df.sort_values(by=['Show Name','Actual Drop Day'])



        # Create columns for percent of show's audience that is male and female
        daily_budget_df['Percent Male'] = daily_budget_df['% M/F'].apply(lambda x: int(x.split('/')[0].split(' ')[1])/100)
        daily_budget_df['Percent Female'] = daily_budget_df['% M/F'].apply(lambda x: int(x.split('/')[1].split(' ')[2])/100)
        


        # Rebuild budget
        rebuilt_budget_df = rebuild_budget(daily_budget_df,'Actual Drop Day')




        # Aggregate Chartable Base File
        chartable_agg_df = chartable_df.groupby(['Date','Ad Campaign Name']).sum()[['Confirmed lead','Estimated lead','Confirmed purchase','Estimated purchase']].reset_index()


 # Define Chartable Pandas SQL
            if client_type == 'Orders Only':

                chartable_code = '''
                SELECT
                    a."Podcast/Station: Account Name",
                    a."Host/Show",
                    a."Network",
                    a."Format",
                    a.Code,
                    a."MF Split",
                    a.Age,
                    a.Day,
                    a."Content Type",
                    a.Chartable,
                    a."Placement Type",
                    a.Placement,
                    a.Product,
                    a.Audience,
                    a."Number of Slots",
                    a."Net Rate per Spot",
                    a."Gross Spot Rate",
                    a."Gross CPM",
                    a.Price,
                    DATE(a.Date) AS "Date",
                    a."Core/Test",
                    a."Opportunity Name",
                    a."Percent Male",
                    a."Percent Female",
                    DATE(a.next_drop_date) AS next_drop_date,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b.Impressions ELSE 0 END) AS impressions,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b.Reach ELSE 0 END) AS reach,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Estimated Unique Visitors" ELSE 0 END) AS estimated_unique_visitors,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Confirmed Unique Visitors" ELSE 0 END) AS confirmed_unique_visitors,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Estimated purchase" ELSE 0 END) AS estimated_purchases,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Confirmed purchase" ELSE 0 END) AS confirmed_purchases,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Estimated Revenue" ELSE 0 END) AS estimated_revenue,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Confirmed Revenue" ELSE 0 END) AS confirmed_revenue
                    
                FROM rebuilt_budget_df a
                    LEFT JOIN chartable_agg_df b ON a."Podcast/Station: Account Name" = b."Ad Campaign Name"
                    
                WHERE 
                    (a.Date <= "{cutoff_date}" AND ((b.Date >= a.Date AND b.Date < a.next_drop_date) OR
                    (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date))) OR
                    (a.Date <= "{cutoff_date}" AND b.Date IS NULL) OR
                    (a.Date <= "{cutoff_date}" AND ((b.Date >= a.Date AND b.Date >= a.next_drop_date) OR 
                    (b.Date <= a.Date AND b.Date <= a.next_drop_date)))
                    
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
                '''.format(cutoff_date=cutoff_date)

            elif client_type == 'Leads Only':

                chartable_code = '''
                SELECT
                    a."Podcast/Station: Account Name",
                    a."Host/Show",
                    a."Network",
                    a."Format",
                    a.Code,
                    a."MF Split",
                    a.Age,
                    a.Day,
                    a."Content Type",
                    a.Chartable,
                    a."Placement Type",
                    a.Placement,
                    a.Product,
                    a.Audience,
                    a."Number of Slots",
                    a."Net Rate per Spot",
                    a."Gross Spot Rate",
                    a."Gross CPM",
                    a.Price,
                    DATE(a.Date) AS "Date",
                    a."Core/Test",
                    a."Opportunity Name",
                    a."Percent Male",
                    a."Percent Female",
                    DATE(a.next_drop_date) AS next_drop_date,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b.Impressions ELSE 0 END) AS impressions,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b.Reach ELSE 0 END) AS reach,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Estimated Unique Visitors" ELSE 0 END) AS estimated_unique_visitors,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Confirmed Unique Visitors" ELSE 0 END) AS confirmed_unique_visitors,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Estimated lead" ELSE 0 END) AS estimated_leads,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Confirmed lead" ELSE 0 END) AS confirmed_leads
                    
                FROM rebuilt_budget_df a
                    LEFT JOIN chartable_agg_df b ON a."Podcast/Station: Account Name" = b."Ad Campaign Name"
                    
                WHERE 
                    (a.Date <= "{cutoff_date}" AND ((b.Date >= a.Date AND b.Date < a.next_drop_date) OR
                    (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date))) OR
                    (a.Date <= "{cutoff_date}" AND b.Date IS NULL) OR
                    (a.Date <= "{cutoff_date}" AND ((b.Date >= a.Date AND b.Date >= a.next_drop_date) OR 
                    (b.Date <= a.Date AND b.Date <= a.next_drop_date)))
                    
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
                '''.format(cutoff_date=cutoff_date)
                
                
                        # Define Chartable Pandas SQL
        # Chartable #
 
            
            else:

                chartable_code = '''
                SELECT
                    a."Show Name",
                    a."Host Name",
                    a."Network",
                    a."Chartable Tracking Y/N",
                    a.Genre,
                    a."Age Demo",
                    a."Ad Type",
                    a."Content Type",
                    a."Test/Core",
                    a.Placement,
                    a.Format,
                    a."Personally Endorsed",
                    a."Downloads",
                    a."Client Rate",
                    DATE(a."Broadcast Week") AS "Broadcast Week",
                    DATE(a."Actual Drop Day") AS "Actual Drop Day",
                    a."Percent Male",
                    a."Percent Female",
                    DATE(a.next_drop_date) AS next_drop_date,
                    SUM(CASE WHEN (b.Date >= a."Actual Drop Day" AND b.Date < a.next_drop_date) OR (a."Actual Drop Day" = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Estimated purchase" ELSE 0 END) AS chartable_estimated_purchases,
                    SUM(CASE WHEN (b.Date >= a."Actual Drop Day" AND b.Date < a.next_drop_date) OR (a."Actual Drop Day" = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Confirmed purchase" ELSE 0 END) AS chartable_confirmed_purchases,
                    SUM(CASE WHEN (b.Date >= a."Actual Drop Day" AND b.Date < a.next_drop_date) OR (a."Actual Drop Day" = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Estimated lead" ELSE 0 END) AS chartable_estimated_leads,
                    SUM(CASE WHEN (b.Date >= a."Actual Drop Day" AND b.Date < a.next_drop_date) OR (a."Actual Drop Day" = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Confirmed lead" ELSE 0 END) AS chartable_confirmed_leads
            
                FROM rebuilt_budget_df a
                    LEFT JOIN chartable_agg_df b ON a."Show Name" = b."Ad Campaign Name"
            
                WHERE 
                    (a."Broadcast Week" <= "{cutoff_date}" AND ((b.Date >= a."Actual Drop Day" AND b.Date < a.next_drop_date) OR
                    (a."Actual Drop Day" = a.next_drop_date AND b.Date >= a.next_drop_date))) OR
                    (a."Broadcast Week" <= "{cutoff_date}" AND b.Date IS NULL) OR
                    (a."Broadcast Week" <= "{cutoff_date}" AND ((b.Date >= a."Actual Drop Day" AND b.Date >= a.next_drop_date) OR 
                    (b.Date <= a."Actual Drop Day" AND b.Date <= a.next_drop_date)))
            
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
                '''.format(cutoff_date=cutoff_date)

            chartable_total_df = ps.sqldf(chartable_code,locals())


            st.write('')
            st.write('')



            ### OUTPUT ###
            st.subheader('Data Source Output')
            st.write('')
            st.write('')

            # Create download link for transactions file
            chartable_csv = chartable_total_df.to_csv(index=False)
            st.download_button(label='Download Chartable Data',data=chartable_csv,file_name='chartable.csv',mime='text/csv')
        
        
		
elif sonic_im_client == 'Justworks':

    st.header('Sonic File Processor')
    st.subheader('File Upload')

    st.write('1. Upload the complete budget from Salesforce')
    uploaded_daily_budget = st.file_uploader(label='',accept_multiple_files=False)

    st.write('')
    st.write('')
    if uploaded_daily_budget is not None:
        st.write('2. Select the client you want to process')
        daily_budget_df = pd.read_csv(uploaded_daily_budget,parse_dates=['Date'])
        client = st.selectbox(label='',options=daily_budget_df['Account Name: Account Name'].unique())
        client_type = st.selectbox(label='',options=['Leads Only','Orders Only','All'])

        st.write('')
        st.write('')
        st.write("3. Upload the client's Chartable data")
        uploaded_chartable_data = st.file_uploader(label='Chartable Data',accept_multiple_files=False)


        st.write('')
        st.write('')
        st.write('4. Select a cutoff date')
        cutoff_date = st.date_input(label='',value=datetime.today().date())


        # Create base Chartable dataframe
        if uploaded_chartable_data is not None:
            chartable_df = pd.read_csv(uploaded_chartable_data, parse_dates=['Date'])


            ## Daily Budget Processing ##
            daily_budget_df = daily_budget_df[daily_budget_df['Account Name: Account Name'] == client].sort_values(by=['Podcast/Station: Account Name','Date'])
            daily_budget_df['Percent Male'] = daily_budget_df['MF Split'].apply(lambda x: int(x.split('/')[0].split(' ')[1])/100)
            daily_budget_df['Percent Female'] = daily_budget_df['MF Split'].apply(lambda x: int(x.split('/')[1].split(' ')[2])/100)
            daily_budget_df['Net Rate per Spot'] = daily_budget_df['Net Rate per Spot'].str.split(" ").str[1] 
            daily_budget_df['Audience'] = pd.to_numeric(daily_budget_df['Audience'])


            # Rebuild budget
            rebuilt_budget_df = rebuild_budget(daily_budget_df,date_series_name='Date',show_series_name='Podcast/Station: Account Name')


            # Aggregate Chartable data
            chartable_agg_df = chartable_df.groupby(['Date','Ad Campaign Name']).sum().reset_index()


            # Define Chartable Pandas SQL
            if client_type == 'Orders Only':

                chartable_code = '''
                SELECT
                    a."Podcast/Station: Account Name",
                    a."Host/Show",
                    a."Network",
                    a."Format",
                    a.Code,
                    a."MF Split",
                    a.Age,
                    a.Day,
                    a."Content Type",
                    a.Chartable,
                    a."Placement Type",
                    a.Placement,
                    a.Product,
                    a.Audience,
                    a."Number of Slots",
                    a."Net Rate per Spot",
                    a."Gross Spot Rate",
                    a."Gross CPM",
                    a.Price,
                    DATE(a.Date) AS "Date",
                    a."Core/Test",
                    a."Opportunity Name",
                    a."Percent Male",
                    a."Percent Female",
                    DATE(a.next_drop_date) AS next_drop_date,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b.Impressions ELSE 0 END) AS impressions,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b.Reach ELSE 0 END) AS reach,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Estimated Unique Visitors" ELSE 0 END) AS estimated_unique_visitors,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Confirmed Unique Visitors" ELSE 0 END) AS confirmed_unique_visitors,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Estimated purchase" ELSE 0 END) AS estimated_purchases,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Confirmed purchase" ELSE 0 END) AS confirmed_purchases,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Estimated Revenue" ELSE 0 END) AS estimated_revenue,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Confirmed Revenue" ELSE 0 END) AS confirmed_revenue
                    
                FROM rebuilt_budget_df a
                    LEFT JOIN chartable_agg_df b ON a."Podcast/Station: Account Name" = b."Ad Campaign Name"
                    
                WHERE 
                    (a.Date <= "{cutoff_date}" AND ((b.Date >= a.Date AND b.Date < a.next_drop_date) OR
                    (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date))) OR
                    (a.Date <= "{cutoff_date}" AND b.Date IS NULL) OR
                    (a.Date <= "{cutoff_date}" AND ((b.Date >= a.Date AND b.Date >= a.next_drop_date) OR 
                    (b.Date <= a.Date AND b.Date <= a.next_drop_date)))
                    
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
                '''.format(cutoff_date=cutoff_date)

            elif client_type == 'Leads Only':

                chartable_code = '''
                SELECT
                    a."Podcast/Station: Account Name",
                    a."Host/Show",
                    a."Network",
                    a."Format",
                    a.Code,
                    a."MF Split",
                    a.Age,
                    a.Day,
                    a."Content Type",
                    a.Chartable,
                    a."Placement Type",
                    a.Placement,
                    a.Product,
                    a.Audience,
                    a."Number of Slots",
                    a."Net Rate per Spot",
                    a."Gross Spot Rate",
                    a."Gross CPM",
                    a.Price,
                    DATE(a.Date) AS "Date",
                    a."Core/Test",
                    a."Opportunity Name",
                    a."Percent Male",
                    a."Percent Female",
                    DATE(a.next_drop_date) AS next_drop_date,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b.Impressions ELSE 0 END) AS impressions,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b.Reach ELSE 0 END) AS reach,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Estimated Unique Visitors" ELSE 0 END) AS estimated_unique_visitors,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Confirmed Unique Visitors" ELSE 0 END) AS confirmed_unique_visitors,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Estimated lead" ELSE 0 END) AS estimated_leads,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Confirmed lead" ELSE 0 END) AS confirmed_leads
                    
                FROM rebuilt_budget_df a
                    LEFT JOIN chartable_agg_df b ON a."Podcast/Station: Account Name" = b."Ad Campaign Name"
                    
                WHERE 
                    (a.Date <= "{cutoff_date}" AND ((b.Date >= a.Date AND b.Date < a.next_drop_date) OR
                    (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date))) OR
                    (a.Date <= "{cutoff_date}" AND b.Date IS NULL) OR
                    (a.Date <= "{cutoff_date}" AND ((b.Date >= a.Date AND b.Date >= a.next_drop_date) OR 
                    (b.Date <= a.Date AND b.Date <= a.next_drop_date)))
                    
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
                '''.format(cutoff_date=cutoff_date)
            
            else:

                chartable_code = '''
                SELECT
                    a."Podcast/Station: Account Name",
                    a."Host/Show",
                    a."Network",
                    a."Format",
                    a.Code,
                    a."MF Split",
                    a.Age,
                    a.Day,
                    a."Content Type",
                    a.Chartable,
                    a."Placement Type",
                    a.Placement,
                    a.Product,
                    a.Audience,
                    a."Number of Slots",
                    a."Net Rate per Spot",
                    a."Gross Spot Rate",
                    a."Gross CPM",
                    a.Price,
                    DATE(a.Date) AS "Date",
                    a."Core/Test",
                    a."Opportunity Name",
                    a."Percent Male",
                    a."Percent Female",
                    DATE(a.next_drop_date) AS next_drop_date,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b.Impressions ELSE 0 END) AS impressions,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b.Reach ELSE 0 END) AS reach,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Estimated Unique Visitors" ELSE 0 END) AS estimated_unique_visitors,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Confirmed Unique Visitors" ELSE 0 END) AS confirmed_unique_visitors,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Estimated purchase" ELSE 0 END) AS estimated_purchases,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Confirmed purchase" ELSE 0 END) AS confirmed_purchases,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Estimated Revenue" ELSE 0 END) AS estimated_revenue,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Confirmed Revenue" ELSE 0 END) AS confirmed_revenue,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Estimated lead" ELSE 0 END) AS estimated_leads,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Confirmed lead" ELSE 0 END) AS confirmed_leads
                    
                FROM rebuilt_budget_df a
                    LEFT JOIN chartable_agg_df b ON a."Podcast/Station: Account Name" = b."Ad Campaign Name"
                    
                WHERE 
                    (a.Date <= "{cutoff_date}" AND ((b.Date >= a.Date AND b.Date < a.next_drop_date) OR
                    (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date))) OR
                    (a.Date <= "{cutoff_date}" AND b.Date IS NULL) OR
                    (a.Date <= "{cutoff_date}" AND ((b.Date >= a.Date AND b.Date >= a.next_drop_date) OR 
                    (b.Date <= a.Date AND b.Date <= a.next_drop_date)))
                    
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
                '''.format(cutoff_date=cutoff_date)

            chartable_total_df = ps.sqldf(chartable_code,locals())


            st.write('')
            st.write('')



            ### OUTPUT ###
            st.subheader('Data Source Output')
            st.write('')
            st.write('')

            # Create download link for transactions file
            chartable_csv = chartable_total_df.to_csv(index=False)
            st.download_button(label='Download Chartable Data',data=chartable_csv,file_name='chartable.csv',mime='text/csv')
			



elif sonic_im_client == 'Other':

    st.header('Sonic File Processor')
    st.subheader('File Upload')

    st.write('1. Upload the complete budget from Salesforce')
    uploaded_daily_budget = st.file_uploader(label='',accept_multiple_files=False)

    st.write('')
    st.write('')
    if uploaded_daily_budget is not None:
        st.write('2. Select the client you want to process')
        daily_budget_df = pd.read_csv(uploaded_daily_budget,parse_dates=['Date'])
        client = st.selectbox(label='',options=daily_budget_df['Account Name: Account Name'].unique())
        client_type = st.selectbox(label='',options=['Leads Only','Orders Only','All'])

        st.write('')
        st.write('')
        st.write("3. Upload the client's Chartable data")
        uploaded_chartable_data = st.file_uploader(label='Chartable Data',accept_multiple_files=False)


        st.write('')
        st.write('')
        st.write('4. Select a cutoff date')
        cutoff_date = st.date_input(label='',value=datetime.today().date())


        # Create base Chartable dataframe
        if uploaded_chartable_data is not None:
            chartable_df = pd.read_csv(uploaded_chartable_data, parse_dates=['Date'])


            ## Daily Budget Processing ##
            daily_budget_df = daily_budget_df[daily_budget_df['Account Name: Account Name'] == client].sort_values(by=['Podcast/Station: Account Name','Date'])
            daily_budget_df['Percent Male'] = daily_budget_df['MF Split'].apply(lambda x: int(x.split('/')[0].split(' ')[1])/100)
            daily_budget_df['Percent Female'] = daily_budget_df['MF Split'].apply(lambda x: int(x.split('/')[1].split(' ')[2])/100)


            # Rebuild budget
            rebuilt_budget_df = rebuild_budget(daily_budget_df,date_series_name='Date',show_series_name='Podcast/Station: Account Name')


            # Aggregate Chartable data
            chartable_agg_df = chartable_df.groupby(['Date','Ad Campaign Name']).sum().reset_index()


            # Define Chartable Pandas SQL
            if client_type == 'Orders Only':

                chartable_code = '''
                SELECT
                    a."Podcast/Station: Account Name",
                    a."Host/Show",
                    a."Network",
                    a."Format",
                    a.Code,
                    a."MF Split",
                    a.Age,
                    a.Day,
                    a."Content Type",
                    a.Chartable,
                    a."Placement Type",
                    a.Placement,
                    a.Product,
                    a.Audience,
                    a."Number of Slots",
                    a."Gross Spot Rate",
                    a."Gross CPM",
                    a.Price,
                    DATE(a.Date) AS "Date",
                    a."Core/Test",
                    a."Opportunity Name",
                    a."Percent Male",
                    a."Percent Female",
                    DATE(a.next_drop_date) AS next_drop_date,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b.Impressions ELSE 0 END) AS impressions,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b.Reach ELSE 0 END) AS reach,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Estimated Unique Visitors" ELSE 0 END) AS estimated_unique_visitors,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Confirmed Unique Visitors" ELSE 0 END) AS confirmed_unique_visitors,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Estimated purchase" ELSE 0 END) AS estimated_purchases,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Confirmed purchase" ELSE 0 END) AS confirmed_purchases,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Estimated Revenue" ELSE 0 END) AS estimated_revenue,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Confirmed Revenue" ELSE 0 END) AS confirmed_revenue
                    
                FROM rebuilt_budget_df a
                    LEFT JOIN chartable_agg_df b ON a."Podcast/Station: Account Name" = b."Ad Campaign Name"
                    
                WHERE 
                    (a.Date <= "{cutoff_date}" AND ((b.Date >= a.Date AND b.Date < a.next_drop_date) OR
                    (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date))) OR
                    (a.Date <= "{cutoff_date}" AND b.Date IS NULL) OR
                    (a.Date <= "{cutoff_date}" AND ((b.Date >= a.Date AND b.Date >= a.next_drop_date) OR 
                    (b.Date <= a.Date AND b.Date <= a.next_drop_date)))
                    
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
                '''.format(cutoff_date=cutoff_date)

            elif client_type == 'Leads Only':

                chartable_code = '''
                SELECT
                    a."Podcast/Station: Account Name",
                    a."Host/Show",
                    a."Network",
                    a."Format",
                    a.Code,
                    a."MF Split",
                    a.Age,
                    a.Day,
                    a."Content Type",
                    a.Chartable,
                    a."Placement Type",
                    a.Placement,
                    a.Product,
                    a.Audience,
                    a."Number of Slots",
                    a."Gross Spot Rate",
                    a."Gross CPM",
                    a.Price,
                    DATE(a.Date) AS "Date",
                    a."Core/Test",
                    a."Opportunity Name",
                    a."Percent Male",
                    a."Percent Female",
                    DATE(a.next_drop_date) AS next_drop_date,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b.Impressions ELSE 0 END) AS impressions,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b.Reach ELSE 0 END) AS reach,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Estimated Unique Visitors" ELSE 0 END) AS estimated_unique_visitors,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Confirmed Unique Visitors" ELSE 0 END) AS confirmed_unique_visitors,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Estimated lead" ELSE 0 END) AS estimated_leads,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Confirmed lead" ELSE 0 END) AS confirmed_leads
                    
                FROM rebuilt_budget_df a
                    LEFT JOIN chartable_agg_df b ON a."Podcast/Station: Account Name" = b."Ad Campaign Name"
                    
                WHERE 
                    (a.Date <= "{cutoff_date}" AND ((b.Date >= a.Date AND b.Date < a.next_drop_date) OR
                    (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date))) OR
                    (a.Date <= "{cutoff_date}" AND b.Date IS NULL) OR
                    (a.Date <= "{cutoff_date}" AND ((b.Date >= a.Date AND b.Date >= a.next_drop_date) OR 
                    (b.Date <= a.Date AND b.Date <= a.next_drop_date)))
                    
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
                '''.format(cutoff_date=cutoff_date)
            
            else:

                chartable_code = '''
                SELECT
                    a."Podcast/Station: Account Name",
                    a."Host/Show",
                    a."Network",
                    a."Format",
                    a.Code,
                    a."MF Split",
                    a.Age,
                    a.Day,
                    a."Content Type",
                    a.Chartable,
                    a."Placement Type",
                    a.Placement,
                    a.Product,
                    a.Audience,
                    a."Number of Slots",
                    a."Gross Spot Rate",
                    a."Gross CPM",
                    a.Price,
                    DATE(a.Date) AS "Date",
                    a."Core/Test",
                    a."Opportunity Name",
                    a."Percent Male",
                    a."Percent Female",
                    DATE(a.next_drop_date) AS next_drop_date,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b.Impressions ELSE 0 END) AS impressions,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b.Reach ELSE 0 END) AS reach,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Estimated Unique Visitors" ELSE 0 END) AS estimated_unique_visitors,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Confirmed Unique Visitors" ELSE 0 END) AS confirmed_unique_visitors,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Estimated purchase" ELSE 0 END) AS estimated_purchases,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Confirmed purchase" ELSE 0 END) AS confirmed_purchases,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Estimated Revenue" ELSE 0 END) AS estimated_revenue,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Confirmed Revenue" ELSE 0 END) AS confirmed_revenue,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Estimated lead" ELSE 0 END) AS estimated_leads,
                    SUM(CASE WHEN (b.Date >= a.Date AND b.Date < a.next_drop_date) OR (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date) THEN b."Confirmed lead" ELSE 0 END) AS confirmed_leads
                    
                FROM rebuilt_budget_df a
                    LEFT JOIN chartable_agg_df b ON a."Podcast/Station: Account Name" = b."Ad Campaign Name"
                    
                WHERE 
                    (a.Date <= "{cutoff_date}" AND ((b.Date >= a.Date AND b.Date < a.next_drop_date) OR
                    (a.Date = a.next_drop_date AND b.Date >= a.next_drop_date))) OR
                    (a.Date <= "{cutoff_date}" AND b.Date IS NULL) OR
                    (a.Date <= "{cutoff_date}" AND ((b.Date >= a.Date AND b.Date >= a.next_drop_date) OR 
                    (b.Date <= a.Date AND b.Date <= a.next_drop_date)))
                    
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
                '''.format(cutoff_date=cutoff_date)

            chartable_total_df = ps.sqldf(chartable_code,locals())


            st.write('')
            st.write('')



            ### OUTPUT ###
            st.subheader('Data Source Output')
            st.write('')
            st.write('')

            # Create download link for transactions file
            chartable_csv = chartable_total_df.to_csv(index=False)
            st.download_button(label='Download Chartable Data',data=chartable_csv,file_name='chartable.csv',mime='text/csv')
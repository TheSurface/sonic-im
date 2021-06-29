import pandas as pd
import streamlit as st
import base64
from datetime import datetime
import math
from IPython.display import HTML
import json


st.set_page_config(page_title='Sonic IM File Processor',layout='wide')
sonic_im_client = st.sidebar.radio('Sonic IM Client',['Keeps','Ten Thousand','Article'])

## FUNCTIONS
# Function to eliminate unnecessary rows after joining the budget to the lead, order, and chartable data sources
def create_download_link(df, title = "Download CSV file", filename = "data.csv", with_index=False):  
    csv = df.to_csv(index = with_index)
    b64 = base64.b64encode(csv.encode())
    payload = b64.decode()
    html = '<a download="{filename}" href="data:text/csv;base64,{payload}" target="_blank">{title}</a>'
    html = html.format(payload=payload,title=title,filename=filename)
    return HTML(html)


def reduce_df(df,show_field_name,date_field_name):
    
    crit_1 = df['Actual Drop Day'] <= cutoff_date
    crit_2 = (df[date_field_name] >= df['Actual Drop Day']) & (df[date_field_name] < df['next_drop_date'])
    crit_3 = (df['Actual Drop Day'] == df['next_drop_date']) & (df[date_field_name] >= df['Actual Drop Day'])
    crit_4 = df[show_field_name].isnull()
    crit_5 = (df[date_field_name] >= df['Actual Drop Day']) & (df[date_field_name] >= df['next_drop_date'])
    crit_6 = (df[date_field_name] <= df['Actual Drop Day']) & (df[date_field_name] <= df['next_drop_date'])
    
    reduced_df = df[(crit_1 & (crit_2 | crit_3)) | (crit_1 & crit_4) | (crit_1 & (crit_5 | crit_6))]
    
    return reduced_df


# Function to rebuild the budget with the Actual Drop Day and Next Drop Day columns
def rebuild_budget(daily_budget_df):

    rebuilt_budget_df = pd.DataFrame()

    for show in daily_budget_df['Show Name'].unique():
        temp_list = []
        temp_df = daily_budget_df[daily_budget_df['Show Name'] == show]
        drop_series = temp_df['Actual Drop Day'].reset_index()

        shifted_drop_series = drop_series.shift(-1)
        index_list = shifted_drop_series.index.values

        for item in index_list:
            if math.isnan(shifted_drop_series['index'][item]):
                temp_list.append(drop_series['Actual Drop Day'][item])
            else:
                temp_list.append(shifted_drop_series['Actual Drop Day'][item])

        temp_df.reset_index(inplace=True)
        temp_df['next_drop_date'] = pd.Series(temp_list)
        temp_df.drop(['index'],axis=1)

        rebuilt_budget_df = pd.concat([rebuilt_budget_df,temp_df],axis=0)

    return rebuilt_budget_df


def zero_out_crit(df):
    crit_5 = (df['event_date'] > df['Actual Drop Day']) & (df['event_date'] >= df['next_drop_date']) & (df['Actual Drop Day'] != df['next_drop_date'])
    crit_6 = (df['event_date'] < df['Actual Drop Day']) & (df['event_date'] <= df['next_drop_date']) & (df['Actual Drop Day'] != df['next_drop_date'])
    crit_7 = (df['Actual Drop Day'] == df['next_drop_date']) & (df['event_date'] < df['Actual Drop Day'])

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
    col1, col2 = st.beta_columns(2)

    with col1:
        uploaded_purchases = st.file_uploader(label='Purchases',accept_multiple_files=False)

    with col2:
        uploaded_leads = st.file_uploader(label='Leads',accept_multiple_files=False)

    uploaded_daily_budget = st.file_uploader(label='Keeps Budget',accept_multiple_files=False)
    uploaded_chartable_data = st.file_uploader(label='Chartable Data',accept_multiple_files=False)



    ### Create Data Frames ###
    if (uploaded_leads is not None) and (uploaded_daily_budget is not None) and (uploaded_purchases is not None) and (uploaded_chartable_data is None):
        looker_file_purchases_df = pd.read_csv(uploaded_purchases,parse_dates=["User's First Non-refunded Purchase Date","Lead Created Date"])
        looker_file_leads_df = pd.read_csv(uploaded_leads,parse_dates=['Lead Created Date'])
        daily_budget_df = pd.read_csv(uploaded_daily_budget,parse_dates=['Broadcast Week','Actual Drop Day'])
    elif (uploaded_leads is not None) and (uploaded_daily_budget is not None) and (uploaded_purchases is not None) and (uploaded_chartable_data is not None):
        looker_file_purchases_df = pd.read_csv(uploaded_purchases,parse_dates=["User's First Non-refunded Purchase Date","Lead Created Date"])
        looker_file_leads_df = pd.read_csv(uploaded_leads,parse_dates=['Lead Created Date'])
        daily_budget_df = pd.read_csv(uploaded_daily_budget,parse_dates=['Broadcast Week','Actual Drop Day'])
        chartable_df = pd.read_csv(uploaded_chartable_data, parse_dates=['Date'])



    ### Create CPO and CPL Extract ###
    if (uploaded_leads is not None) and (uploaded_daily_budget is not None) and (uploaded_purchases is not None) and (uploaded_chartable_data is None):

        # Create DataFrames from uploaded CSV files
        daily_budget_df = daily_budget_df.sort_values(by=['Show Name','Actual Drop Day'])


        # Drop unused columns
        looker_file_purchases_df.drop(labels=['Unnamed: 0','User ID','Lead Created Date','Utm Source'],axis=1,inplace=True)
        looker_file_leads_df.drop(labels=['Unnamed: 0','User ID','Utm Source'],axis=1,inplace=True)


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
        rebuilt_budget_df = rebuild_budget(daily_budget_df)


        # Create final DataFrames to export for Tableau data sources
        purchases_df = pd.merge(rebuilt_budget_df,looker_purchases_agg_df,left_on=['UTM'],right_on=['Utm Campaign'],how='left')
        leads_df = pd.merge(rebuilt_budget_df,looker_leads_agg_df,left_on=['UTM'],right_on=['Utm Campaign'],how='left')


        # Change date column name to event_date
        purchases_df.rename({"User's First Non-refunded Purchase Date":'event_date'}, axis=1, inplace=True)
        leads_df.rename({'Lead Created Date':'event_date'}, axis=1, inplace=True)


        # Convert date fields to just dates
        purchases_df['event_date'] = purchases_df['event_date'].apply(lambda x: x.date())
        purchases_df['Actual Drop Day'] = purchases_df['Actual Drop Day'].apply(lambda x: x.date())
        purchases_df['next_drop_date'] = purchases_df['next_drop_date'].apply(lambda x: x.date())

        leads_df['event_date'] = leads_df['event_date'].apply(lambda x: x.date())
        leads_df['Actual Drop Day'] = leads_df['Actual Drop Day'].apply(lambda x: x.date())
        leads_df['next_drop_date'] = leads_df['next_drop_date'].apply(lambda x: x.date())


        # Create final leads and purchases DataFrames by eliminating negative date_diffs and including drops which had 0 leads or order
        leads_df.loc[zero_out_crit(leads_df)[0] | zero_out_crit(leads_df)[1] | zero_out_crit(leads_df)[2],'leads'] = 0
        purchases_df.loc[zero_out_crit(purchases_df)[0] | zero_out_crit(purchases_df)[1] | zero_out_crit(purchases_df)[2],'orders'] = 0

        final_purchases_df = reduce_df(purchases_df,'Utm Campaign','event_date')
        final_purchases_df.fillna(value={'orders':0},inplace=True)

        final_leads_df = reduce_df(leads_df,'Utm Campaign','event_date')
        final_leads_df.fillna(value={'leads':0},inplace=True)


        
        st.write('')
        st.write('')


        ### OUTPUT ###
        st.subheader('Data Source Output')
        st.write('')
        st.write('')

        # Create download link for transactions file
        orders_csv = final_purchases_df.to_csv(index=False)
        b64 = base64.b64encode(orders_csv.encode()).decode()  # some strings <-> bytes conversions necessary here
        orders_href = f'<a href="data:file/csv;base64,{b64}" download="orders.csv">Download your Orders CSV File</a> (right-click and save)'
        st.markdown(orders_href, unsafe_allow_html=True)

        leads_csv = final_leads_df.to_csv(index=False)
        b64 = base64.b64encode(leads_csv.encode()).decode()  # some strings <-> bytes conversions necessary here
        leads_href = f'<a href="data:file/csv;base64,{b64}" download="leads.csv">Download your Leads CSV File</a> (right-click and save)'
        st.markdown(leads_href, unsafe_allow_html=True)



    ### Create Chartable Extract ###
    elif (uploaded_leads is not None) and (uploaded_daily_budget is not None) and (uploaded_purchases is not None) and (uploaded_chartable_data is not None):

        ## Chartable Processing ##
        chartable_df['show_name'] = chartable_df['Ad Campaign Name'].apply(lambda x: x.split(' - ')[0])
        chartable_df['event_date'] = chartable_df['Date']

        chartable_lead_df = chartable_df.groupby(['event_date','show_name']).sum()['Confirmed lead'].reset_index().rename({'Confirmed lead':'count'},axis=1)
        chartable_lead_df['event_type'] = 'lead'

        chartable_purchase_df = chartable_df.groupby(['event_date','show_name']).sum()['Confirmed Conversions'].reset_index().rename({'Confirmed Conversions':'count'},axis=1)
        chartable_purchase_df['event_type'] = 'purchase'

        chartable_df = pd.concat([chartable_lead_df,chartable_purchase_df])
        chartable_df['source'] = 'Chartable'


        ## Looker Processing ##
        # Leads
        looker_file_leads_df['Lead Created Date'] = looker_file_leads_df['Lead Created Date'].apply(lambda x: x.date())
        looker_file_leads_df['event_type'] = 'lead'
        looker_leads_df = looker_file_leads_df.groupby(['Lead Created Date','Utm Campaign','event_type']).count()['Utm Source'].reset_index()
        looker_leads_df.rename({'Utm Source': 'count', 'Lead Created Date': 'event_date','Utm Campaign':'show_name'}, axis=1, inplace=True)
        looker_leads_df['source'] = 'Looker'

        # Purchases
        looker_file_purchases_df["User's First Non-refunded Purchase Date"] = looker_file_purchases_df['User\'s First Non-refunded Purchase Date'].apply(lambda x: x.date())
        looker_file_purchases_df['event_type'] = 'purchase'
        looker_purchases_df = looker_file_purchases_df.groupby(["User's First Non-refunded Purchase Date",'Utm Campaign','event_type']).count()['Utm Source'].reset_index()
        looker_purchases_df.rename({'Utm Source':'count',"User's First Non-refunded Purchase Date": 'event_date','Utm Campaign':'show_name'}, axis=1, inplace=True)
        looker_purchases_df['source'] = 'Looker'

        # Concatenate Together
        looker_all_df = pd.concat([looker_leads_df,looker_purchases_df])


        ## Daily Budget Processing ##
        daily_budget_df['Actual Drop Day'] = daily_budget_df['Actual Drop Day'].apply(lambda x: x.date())

        # Rebuild budget
        rebuilt_budget_df = rebuild_budget(daily_budget_df)


        ## Creation of final files ##
        chartable_final_df = pd.merge(rebuilt_budget_df, chartable_df, left_on=['Show Name'], right_on=['show_name'], how='left')
        chartable_final_df = chartable_final_df[~chartable_final_df['count'].isnull()]
        
        looker_final_df = pd.merge(rebuilt_budget_df,looker_all_df, left_on=['UTM'], right_on=['show_name'], how='left')


        ## Reduce rows and group records ##
        def zero_out_crit(df):
            crit_5 = (df['event_date'] > df['Actual Drop Day']) & (df['event_date'] >= df['next_drop_date']) & (df['Actual Drop Day'] != df['next_drop_date'])
            crit_6 = (df['event_date'] < df['Actual Drop Day']) & (df['event_date'] <= df['next_drop_date']) & (df['Actual Drop Day'] != df['next_drop_date'])
            crit_7 = (df['Actual Drop Day'] == df['next_drop_date']) & (df['event_date'] < df['Actual Drop Day'])

            return [crit_5,crit_6,crit_7]

        looker_final_df.loc[zero_out_crit(looker_final_df)[0] | zero_out_crit(looker_final_df)[1] | zero_out_crit(looker_final_df)[2],'count'] = 0
        chartable_final_df.loc[zero_out_crit(chartable_final_df)[0] | zero_out_crit(chartable_final_df)[1] | zero_out_crit(chartable_final_df)[2],'count'] = 0

        chartable_final_df = reduce_df(chartable_final_df,'show_name','event_date')
        chartable_final_grp_df = chartable_final_df.groupby(['source','Show Name','Actual Drop Day','next_drop_date','Client Rate','event_type']).sum()['count'].reset_index()

        
        looker_final_df = reduce_df(looker_final_df,'show_name','event_date')
        looker_final_df = looker_final_df[looker_final_df['Show Name'].isin(chartable_final_df['Show Name'].unique())]
        looker_final_grp_df = looker_final_df.groupby(['source','Show Name','Actual Drop Day','next_drop_date','Client Rate','event_type']).sum()['count'].reset_index()


        ## Produce final data frame ##
        final_df = pd.concat([chartable_final_grp_df, looker_final_grp_df]).reset_index()

        st.write('')
        st.write('')



        ### OUTPUT ###
        st.subheader('Data Source Output')
        st.write('')
        st.write('')

        # Create download link for transactions file
        chartable_csv = final_df.to_csv(index=False)
        b64 = base64.b64encode(chartable_csv.encode()).decode()  # some strings <-> bytes conversions necessary here
        chartable_href = f'<a href="data:file/csv;base64,{b64}">Download your Chartable CSV File</a> (right-click and save as &lt;some_name&gt;.csv)'
        st.markdown(chartable_href, unsafe_allow_html=True)
    
elif sonic_im_client == 'Ten Thousand':

    st.header('Ten Thousand File Processor')
    st.subheader('File Upload')
    st.write('1. Upload the following three files (Leads, Purchases, Ten Thousand Daily Budget, Chartable Data) using the widgets below')
    st.write('2. Select a cutoff date')
    cutoff_date = st.date_input(label='',value=datetime.today().date())
    st.write('')
    st.write('')
    st.write('The output will be a file which can be downloaded and used to update the Ten Thousand Tableau dashboard.')
    st.write('')
    st.write('')

    col1, col2 = st.beta_columns(2)
    with col1:
        uploaded_client_data = st.file_uploader(label='Ten Thousand Client Data',accept_multiple_files=False)

    with col2:
        uploaded_daily_budget = st.file_uploader(label='Ten Thousand Budget',accept_multiple_files=False)

    uploaded_chartable_data = st.file_uploader(label='Chartable Data',accept_multiple_files=False)

     ### Create Data Frames ###
    if (uploaded_client_data is not None) and (uploaded_daily_budget is not None):
        daily_budget_df = pd.read_csv(uploaded_daily_budget,parse_dates=['Broadcast Week'])
        tt_client_data_df = pd.read_csv(uploaded_client_data,parse_dates=['day'])

    elif (uploaded_daily_budget is not None) and (uploaded_chartable_data is not None):
        daily_budget_df = pd.read_csv(uploaded_daily_budget,parse_dates=['Broadcast Week'])
        chartable_df = pd.read_csv(uploaded_chartable_data, parse_dates=['Date'])



    ### Create CPO and CPL Extract ###
    if (uploaded_daily_budget is not None)  and (uploaded_client_data is not None):

        # Create DataFrames from uploaded CSV files
        daily_budget_df = daily_budget_df.sort_values(by=['Show Name','Broadcast Week'])


        # Create columns for percent of show's audience that is male and female
        daily_budget_df['Percent Male'] = daily_budget_df['% M/F'].apply(lambda x: int(x.split('/')[0].strip('M'))/100)
        daily_budget_df['Percent Female'] = daily_budget_df['% M/F'].apply(lambda x: int(x.split('/')[1].strip('F'))/100)


        # Rebuild budget
        rebuilt_budget_df = rebuild_budget(daily_budget_df)


        # Create final DataFrames to export for Tableau data sources
        transactions_df = pd.merge(rebuilt_budget_df,tt_client_data_df,left_on=['Vanity URL'],right_on=['name'],how='left')


        # Change date column name to event_date
        transactions_df.rename({'day':'event_date'}, axis=1, inplace=True)


        # Convert date fields to just dates
        transactions_df['event_date'] = transactions_df['event_date'].apply(lambda x: x.date())
        transactions_df['Broadcast Week'] = transactions_df['Broadcast Week'].apply(lambda x: x.date())
        transactions_df['next_drop_date'] = transactions_df['next_drop_date'].apply(lambda x: x.date())


        # Create final leads and purchases DataFrames by eliminating negative date_diffs and including drops which had 0 leads or order
        transactions_df.loc[zero_out_crit(transactions_df)[0] | zero_out_crit(transactions_df)[1] | zero_out_crit(transactions_df)[2],'orders'] = 0
        final_transactions_df = reduce_df(transactions_df,'name','event_date')
        final_transactions_df.fillna(value={'orders':0},inplace=True)
        
        st.write('')
        st.write('')


        ### OUTPUT ###
        st.subheader('Data Source Output')
        st.write('')
        st.write('')

        # Create download link for transactions file
        client_data_csv = final_transactions_df.to_csv(index=False)
        b64 = base64.b64encode(client_data_csv.encode()).decode()  # some strings <-> bytes conversions necessary here
        client_data_href = f'<a href="data:file/csv;base64,{b64}">Download your Client Data CSV File</a> (right-click and save as &lt;some_name&gt;.csv)'
        st.markdown(client_data_href, unsafe_allow_html=True)


    elif (uploaded_daily_budget is not None) and (uploaded_chartable_data is not None):


        ## Daily Budget Processing ##
        daily_budget_df = daily_budget_df.sort_values(by=['Show Name','Broadcast Week'])
        daily_budget_df['Broadcast Week'] = daily_budget_df['Broadcast Week'].apply(lambda x: x.date())
        daily_budget_df['Percent Male'] = daily_budget_df['% M/F'].apply(lambda x: int(x.split('/')[0].strip('M'))/100)
        daily_budget_df['Percent Female'] = daily_budget_df['% M/F'].apply(lambda x: int(x.split('/')[1].strip('F'))/100)


        # Rebuild budget
        rebuilt_budget_df = rebuild_budget(daily_budget_df)


        ## Creation of final files ##
        chartable_final_df = pd.merge(rebuilt_budget_df, chartable_df, left_on=['Show Name'], right_on=['Ad Campaign Name'], how='left')
        #chartable_final_df = chartable_final_df[~chartable_final_df['Estimated Revenue'].isnull()]


        ## Reduce rows and group records ##
        def zero_out_crit(df):
            crit_5 = (df['Date'] > df['Broadcast Week']) & (df['Date'] >= df['next_drop_date']) & (df['Broadcast Week'] != df['next_drop_date'])
            crit_6 = (df['Date'] < df['Broadcast Week']) & (df['Date'] <= df['next_drop_date']) & (df['Broadcast Week'] != df['next_drop_date'])
            crit_7 = (df['Broadcast Week'] == df['next_drop_date']) & (df['Date'] < df['Broadcast Week'])

            return [crit_5,crit_6,crit_7]

        chartable_final_df.loc[zero_out_crit(chartable_final_df)[0] | zero_out_crit(chartable_final_df)[1] | zero_out_crit(chartable_final_df)[2],'Confirmed Unique Households'] = 0
        chartable_final_df.loc[zero_out_crit(chartable_final_df)[0] | zero_out_crit(chartable_final_df)[1] | zero_out_crit(chartable_final_df)[2],'Estimated Unique Households'] = 0
        chartable_final_df.loc[zero_out_crit(chartable_final_df)[0] | zero_out_crit(chartable_final_df)[1] | zero_out_crit(chartable_final_df)[2],'Confirmed Unique Visitors'] = 0
        chartable_final_df.loc[zero_out_crit(chartable_final_df)[0] | zero_out_crit(chartable_final_df)[1] | zero_out_crit(chartable_final_df)[2],'Estimated Unique Visitors'] = 0
        chartable_final_df.loc[zero_out_crit(chartable_final_df)[0] | zero_out_crit(chartable_final_df)[1] | zero_out_crit(chartable_final_df)[2],'Confirmed purchase'] = 0
        chartable_final_df.loc[zero_out_crit(chartable_final_df)[0] | zero_out_crit(chartable_final_df)[1] | zero_out_crit(chartable_final_df)[2],'Estimated purchase'] = 0
        chartable_final_df.loc[zero_out_crit(chartable_final_df)[0] | zero_out_crit(chartable_final_df)[1] | zero_out_crit(chartable_final_df)[2],'Confirmed Conversions'] = 0
        chartable_final_df.loc[zero_out_crit(chartable_final_df)[0] | zero_out_crit(chartable_final_df)[1] | zero_out_crit(chartable_final_df)[2],'Estimated Conversions'] = 0
        chartable_final_df.loc[zero_out_crit(chartable_final_df)[0] | zero_out_crit(chartable_final_df)[1] | zero_out_crit(chartable_final_df)[2],'Impressions'] = 0
        chartable_final_df.loc[zero_out_crit(chartable_final_df)[0] | zero_out_crit(chartable_final_df)[1] | zero_out_crit(chartable_final_df)[2],'Reach'] = 0

        chartable_final_df = reduce_df(chartable_final_df,'Ad Campaign Name','Date')


        st.write('')
        st.write('')



        ### OUTPUT ###
        st.subheader('Data Source Output')
        st.write('')
        st.write('')

        # Create download link for transactions file
        chartable_csv = chartable_final_df.to_csv(index=False)
        b64 = base64.b64encode(chartable_csv.encode()).decode()  # some strings <-> bytes conversions necessary here
        chartable_href = f'<a href="data:file/csv;base64,{b64}">Download your Chartable CSV File</a> (right-click and save as &lt;some_name&gt;.csv)'
        st.markdown(chartable_href, unsafe_allow_html=True)


elif sonic_im_client == 'Article':

    st.header('Article File Processor')
    st.subheader('File Upload')

    st.write('# Under Construction')
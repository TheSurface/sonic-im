import pandas as pd
import streamlit as st
import base64
from datetime import datetime


st.set_page_config(page_title='Keeps File Processor',layout='wide')
st.header('Keeps File Processor')
st.subheader('File Upload')
st.write('Upload the following four files (Leads, Purchases, Keeps Daily Budget, UTM Bridge File) using the widgets below.')
st.write('The output will be a file which can be downloaded and used to update the Keeps Tableau dashboard.')
st.write('')
st.write('')

col1, col2 = st.beta_columns(2)

with col1:
    uploaded_purchases = st.file_uploader(label='Purchases',accept_multiple_files=False)

with col2:
    uploaded_leads = st.file_uploader(label='Leads',accept_multiple_files=False)

uploaded_daily_budget = st.file_uploader(label='Keeps Budget',accept_multiple_files=False)


if (uploaded_leads is not None) and (uploaded_daily_budget is not None) and (uploaded_purchases is not None):

    # Create DataFrames from uploaded CSV files
    looker_file_purchases_df = pd.read_csv(uploaded_purchases,parse_dates=["User's First Non-refunded Purchase Date","Lead Created Date"])
    looker_file_leads_df = pd.read_csv(uploaded_leads,parse_dates=['Lead Created Date'])
    daily_budget_df = pd.read_csv(uploaded_daily_budget,parse_dates=['Broadcast Week','Actual Drop Day'])


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


    # Create final DataFrames to export for Tableau data sources
    purchases_df = pd.merge(daily_budget_df,looker_purchases_agg_df,left_on=['UTM'],right_on=['Utm Campaign'],how='left')
    leads_df = pd.merge(daily_budget_df,looker_leads_agg_df,left_on=['UTM'],right_on=['Utm Campaign'],how='left')


    # Create Date Diff field to attribute orders to the appropriate drop window
    purchases_df['date_diff'] = purchases_df['User\'s First Non-refunded Purchase Date'] - purchases_df['Actual Drop Day']
    leads_df['date_diff'] = leads_df['Lead Created Date'] - leads_df['Actual Drop Day']


    # Create final leads and purchases DataFrames by eliminating negative date_diffs and including drops which had 0 leads or orders
    final_purchases_df = purchases_df[(purchases_df['date_diff'] >= '0 days') | ((purchases_df['orders'].isnull()) & (purchases_df['Actual Drop Day'] <= datetime.today()))]
    final_purchases_df.fillna(value={'orders':0},inplace=True)

    final_leads_df = leads_df[(leads_df['date_diff'] >= '0 days') | ((leads_df['leads'].isnull()) & (leads_df['Actual Drop Day'] <= datetime.today()))]
    final_leads_df.fillna(value={'leads':0},inplace=True)

    
    st.write('')
    st.write('')

    st.subheader('Data Source Output')
    st.write('')
    st.write('')

    # Create download link for transactions file
    orders_csv = final_purchases_df.to_csv(index=False)
    b64 = base64.b64encode(orders_csv.encode()).decode()  # some strings <-> bytes conversions necessary here
    orders_href = f'<a href="data:file/csv;base64,{b64}">Download your Orders CSV File</a> (right-click and save as &lt;some_name&gt;.csv)'
    st.markdown(orders_href, unsafe_allow_html=True)

    leads_csv = final_leads_df.to_csv(index=False)
    b64 = base64.b64encode(leads_csv.encode()).decode()  # some strings <-> bytes conversions necessary here
    leads_href = f'<a href="data:file/csv;base64,{b64}">Download your Leads CSV File</a> (right-click and save as &lt;some_name&gt;.csv)'
    st.markdown(leads_href, unsafe_allow_html=True)
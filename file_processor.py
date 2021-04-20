import pandas as pd
import streamlit as st
import base64

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
    uploaded_leads = st.file_uploader(label='Leads',accept_multiple_files=False)

with col2:
    uploaded_daily_budget = st.file_uploader(label='Keeps Daily Budget',accept_multiple_files=False)
    uploaded_bridge_file = st.file_uploader(label='UTM Bridge File',accept_multiple_files=False)


if (uploaded_leads is not None) and (uploaded_daily_budget is not None) and (uploaded_purchases is not None) and (uploaded_bridge_file is not None):

    # Create DataFrames from uploaded CSV files
    looker_file_purchases_df = pd.read_csv(uploaded_purchases,parse_dates=["User's First Non-refunded Purchase Date","Lead Created Date"])
    looker_file_leads_df = pd.read_csv(uploaded_leads,parse_dates=['Lead Created Date'])
    bridge_df = pd.read_csv(uploaded_bridge_file)
    daily_budget_df = pd.read_csv(uploaded_daily_budget)


    # Drop unused columns
    looker_file_purchases_df.drop(labels=['Unnamed: 0','User ID','Lead Created Date','Utm Source'],axis=1,inplace=True)
    looker_file_leads_df.drop(labels=['Unnamed: 0','User ID','Utm Source'],axis=1,inplace=True)

    # Merge bridge and purchase/lead files together
    looker_file_w_names_purchases_df = pd.merge(looker_file_purchases_df,bridge_df,left_on='Utm Campaign',right_on='UTM',how='left')
    looker_file_w_names_leads_df = pd.merge(looker_file_leads_df,bridge_df,left_on='Utm Campaign',right_on='UTM',how='left')

    # Create purchase and lead indicator column
    looker_file_w_names_purchases_df['orders'] = 1
    looker_file_w_names_leads_df['leads'] = 1

    # Aggregate purchase and lead data by date and show name
    looker_purchases_agg_df = looker_file_w_names_purchases_df.groupby(["User's First Non-refunded Purchase Date",'Podcast Title']).sum()['orders'].reset_index()
    looker_leads_agg_df = looker_file_w_names_leads_df.groupby(['Lead Created Date','Podcast Title']).sum()['leads'].reset_index()

    # Convert the Daily Budget DataFrame from wide form to long form
    daily_budget_processed_df = pd.melt(daily_budget_df,id_vars=daily_budget_df.columns[0:18],value_vars=daily_budget_df.columns[18:354],var_name='Date',value_name='Budget')

    # Create columns for percent of show's audience that is male and female
    daily_budget_processed_df['Percent Male'] = daily_budget_processed_df['% M/F'].apply(lambda x: int(x.split('/')[0].strip('M'))/100)
    daily_budget_processed_df['Percent Female'] = daily_budget_processed_df['% M/F'].apply(lambda x: int(x.split('/')[1].strip('F'))/100)

    # Convert 'Date' column to datetime format
    daily_budget_processed_df['Date'] = daily_budget_processed_df['Date'].apply(pd.to_datetime)

    # Create final DataFrames to export for Tableau data sources
    purchases_df = pd.merge(daily_budget_processed_df,looker_purchases_agg_df,left_on=['Date','Show Name'],right_on=["User's First Non-refunded Purchase Date",'Podcast Title'],how='left')
    leads_df = pd.merge(daily_budget_processed_df,looker_leads_agg_df,left_on=['Date','Show Name'],right_on=['Lead Created Date','Podcast Title'],how='left')

    # Fill in any null values with zeroes
    purchases_df.fillna(value={'orders':0},inplace=True)
    leads_df.fillna(value={'leads':0},inplace=True)

    # Combine leads and purchases into one file
    transactions_df = pd.merge(purchases_df,leads_df[['Date','Show Name','leads']],on=['Date','Show Name'],how='left')

    
    st.write('')
    st.write('')

    st.subheader('Data Source Output')
    st.write('')
    st.write('')

    # Create download link for transactions file
    csv = looker_purchases_agg_df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()  # some strings <-> bytes conversions necessary here
    href = f'<a href="data:file/csv;base64,{b64}">Download CSV File</a> (right-click and save as &lt;some_name&gt;.csv)'
    st.markdown(href, unsafe_allow_html=True)


else:
    st.write('One of the files is coming in as None')
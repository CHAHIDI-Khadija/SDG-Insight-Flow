import streamlit as st
from scopus import  preprocess,upload
import pandas as pd
st.title('Power BI dashboard automation')

uploaded_file = st.file_uploader(label='upload your data source',type=['csv'])

if uploaded_file is not None:
    # Read the uploaded file
    data = pd.read_csv(uploaded_file)
    # Process the data using your 'preprocess' function from 'scopus.py'
    author_data, affiliation_data, meta_data = preprocess(data)
    option = st.selectbox('if tables already exist', 
                        [None,'replace','append'],
                        0)
    if option is not None:
        st.write('You selected:', option)
        upload(author_data, affiliation_data, meta_data,option)

        st.write('Data Successfully upload to Postgres DataBase')
    
        

else:
    st.warning("Please upload a CSV file.")

import pandas as pd
import openpyxl
from NavTools import nav_connect as nv
from datetime import date
import streamlit as st
from featureCostFuntions import * 
import pyautogui
import time
import io

st.set_page_config(layout="wide")

st.title("Feature Cost Test Web Interface")

buffer = io.BytesIO()

with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
    template.to_excel(writer)
    writer.save()
    st.download_button("Feature Cost Template Download", data=buffer, file_name='FeatureCostTemplate.xlsx', mime='application/vnd.ms-excel')

with st.expander("Process Notes"):
    st.write('''
        Upload your completed feature cost template. When a file is uploaded, the costing logic will begin immediately. After the process finishes, review the result and populate any empty cells (besides the contract message column) or any cells containing an analysis warning. Be sure to populate all columns if a new row is added (again, besides the contract message column). Ater all values are populated, click the submission button where your final cost results will be sent and stored. Rows can be deleted if the row's checkmark in the left margin is selected and the 'Delete' key is pressed. 
    ''')

if 'query' not in st.session_state:
    st.session_state.standard = 1
    costRunFile = st.file_uploader("Upload Completed Cost Run Excel File:", type=['csv', 'xlsx'])
    if costRunFile is not None:
        preSlice = {}
        doneDone, standardDone = fileSkim(costRunFile, preSlice)
        st.session_state["preChangeOpt"] = doneDone
        st.session_state["preChangeStand"] = standardDone
        st.session_state["standard"] = standardDone
        st.session_state["optional"] = doneDone
        st.session_state['query'] = 1

if st.session_state.standard.__class__.__name__ == 'DataFrame':
    col1, col2 = st.columns(2)
    standDF = st.session_state.standard
    optionalDF = st.session_state.optional
    col2.header("Optional Feature")
    output1 = col2.experimental_data_editor(optionalDF, use_container_width=True,num_rows="dynamic")
    col1.header("Standard Feature")
    output2 = col1.experimental_data_editor(standDF, use_container_width=True, num_rows="dynamic")
    with st.form("my_form"):
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            output1.to_excel(writer)
            output2.to_excel(writer)
            writer.save()
            col2.download_button("Optional Feature Result Download", data=buffer, file_name='optionalFeat.xlsx', mime='application/vnd.ms-excel')
            col1.download_button("Standard Feature Result Download", data=buffer, file_name='standardFeat.xlsx', mime='application/vnd.ms-excel')
            
            if output1['part_cost_sum'].isnull().values.any() or '****No Contract Price****' in output1['part_cost_sum'].values or output2['part_cost_sum'].isnull().values.any() or '****No Contract Price****' in output2['part_cost_sum'].values:
                col1.error("Populate empty Cost cell to display feature totals")
            else:
                stand = output2['part_cost_sum'].astype('float').sum()
                option = output1['part_cost_sum'].astype('float').sum()

                standTotal = '${:,.2f}'.format(stand)
                optionTotal = '${:,.2f}'.format(option)

                col1.metric("Standard Feature Cost Total",standTotal)
                col2.metric("Optional Feature Cost Total",optionTotal)
        submitted = st.form_submit_button("Submit")
        if submitted:
            if output1['part_cost_sum'].isnull().values.any() or '****No Contract Price****' in output1['part_cost_sum'].values or output2['part_cost_sum'].isnull().values.any() or '****No Contract Price****' in output2['part_cost_sum'].values:
                st.error("Empty cells found. See 'contract_message' column, enter values, and resubmit")
            else: 
            ######### Send to hadoop here #########
                dfDiffCheck(output1, st.session_state["preChangeOpt"])
                dfDiffCheck(output2, st.session_state["preChangeStand"])
                hadoopSend(output1, 1)
                hadoopSend(output2, 0)
                st.balloons()
                time.sleep(1.5)
                pyautogui.hotkey("ctrl","F5")
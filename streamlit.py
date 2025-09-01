import streamlit as st
import time

st.markdown("<h1 style='text-align: center;'>Data Ingestion tool</h1>", unsafe_allow_html=True)

# Custom CSS for the red button
button_style = """
<style>
div.stButton > button:first-child {
    background-color: #f44336;
    color: white;
    font-size: 20px;
    height: 8em;
    width: 8em;
    border-radius: 50%;
    position: absolute;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
}
</style>
"""
st.markdown(button_style, unsafe_allow_html=True)

if st.button("press me"):
    with st.spinner('Loading...'):
        time.sleep(10)
    st.success('Done!')
    st.balloons() 

 # Celebration balloonsst.progress(10)  # Progress barwith st.spinner('Wait for it...'):    time.sleep(10)  # Simulating a process delay
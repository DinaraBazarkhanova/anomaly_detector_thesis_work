import streamlit as st
import pandas as pd
import time
import matplotlib.pyplot as plt


def main() -> None:
    st.title('Anomaly Detector')
    st.write('''Anomaly Detector is an AI service with an API that allows you to track and detect anomalies in time 
    series data.The main goal of anomaly detection is to discover unexpected events or rare occurrences in data.''')
    left_column, right_column = st.columns(2)
    with left_column:
        uploaded_file = st.file_uploader('Upload your **CSV/JSON** file here:')
    if uploaded_file is None:
        st.session_state['upload_state'] = 'Upload a file first!'

    elif uploaded_file.name.endswith('.json'):
        st.header('JSON file Processing')
        df = pd.read_json(uploaded_file)
        # general alternative way
        df['_time'] = pd.to_datetime(df['_time'])
        st.write(df.describe())
        create_plot(df)

    elif uploaded_file.name.endswith('.csv'):
        st.header('CSV file Processing')
        df = pd.read_csv(uploaded_file)
        st.write(df.describe())
        create_plot(df)

    with right_column:
        url = st.text_input('Upload MQTT url path here:')

    if url is None:
        st.info("Please fill in a URL")
        st.stop()


def create_plot(file):
    fig, ax = plt.subplots(1, 1)
    ax.scatter(x=file.iloc[0], y=file.iloc[1])
    ax.set_xlabel('Xlabel')
    ax.set_ylabel('Ylabel')
    st.pyplot(fig)








# def from_data_file(filename):
#     url = (
#             "http://raw.githubusercontent.com/streamlit/"
#             "example-data/master/hello/v1/%s" % filename
#     )
#     return pd.read_json(url)

if __name__ == "__main__":
    main()

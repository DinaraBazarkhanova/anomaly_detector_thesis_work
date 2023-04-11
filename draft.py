from typing import Union
import sys
import time
import pandas as pd
import json
import os
from typing import List
import datetime as dt
from paho.mqtt.client import MQTTMessage
from river import anomaly
from streamz import Stream
import streamlit as st


def process_limits_streaming(topic: str, data: Union[str, pd.DataFrame]):
    port = 8000
    model = OneClassSVM.model = QuantileFilter(OneClassSVM(nu=0.2), q=0.995)

    if isinstance(data, str):
        source = Stream.from_mqtt(data, port, topic)
    elif isinstance(data, pd.DataFrame):
        source = Stream.from_iterable(data.iterrows())
    else:
        raise (RuntimeError("Wrong input_data format."))

    detector = source.map(preprocess, [topic]).map(fit_transform, model)

    with open("dynamic_limits.json", 'a') as f:
        if isinstance(data, str):
            detector.map(lambda x: json.dumps(x)).to_mqtt(data, port, f"{topic.rsplit('/', 1)[0]}/dynamic_limits",
                                                          publish_kwargs={"retain": True})
        elif isinstance(data, pd.DataFrame):
            detector.sink(dump_to_file, f)

    source.start()

    signal.signal(signal.SIGINT, lambda signalnum, frame: signal_handler(signalnum, frame, source, data))

    while True:
        time.sleep(2)


def signal_handler(signalnum, frame, source, data):
    source.stop()
    st.write(f"{data} stopped")


st.title("Anomaly Detection with Streamlit")

data_type = st.radio("Select input_data type", ("MQTT Topic", "Pandas DataFrame"))

if data_type == "MQTT Topic":
    st.sidebar.subheader("MQTT Settings")
    mqtt_host = st.sidebar.text_input("Host", "mqtt.example.com")
    mqtt_port = st.sidebar.number_input("Port", 1883)
    mqtt_topic = st.text_input("Enter MQTT topic")
    process_limits_streaming(mqtt_topic, mqtt_topic)

elif data_type == "Pandas DataFrame":
    uploaded_file = st.file_uploader("Choose a file")
    if uploaded_file is not None:
        data = pd.read_csv(uploaded_file)
        process_limits_streaming("dataframe", data)

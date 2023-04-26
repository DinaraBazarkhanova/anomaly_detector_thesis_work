import paho.mqtt.subscribe as subscribe
import streamlit as st
from streamz import Stream

broker = "mqtt.cloud.uiam.sk"
port = 1883
topics = ["shellies/Shelly3EM-Main-Switchboard-C/emeter/0/power"]
client_id = "marek"

chart = st.line_chart()


def update_chart(x, chart):
    print(f"We wanna update chart with {x}")
    chart.add_rows({k: [v] for k, v in x.items()})


def print_msg(client, userdata, message, s):
    data = message.payload.decode('utf-8')
    print("%s : %s" % (message.topic, data))
    s.emit({message.topic: data})


s = Stream()
s = s.map(lambda x: {'data': x})
s.sink(update_chart, chart)

subscribe.callback(
    lambda x, y, z: print_msg(x, y, z, s), topics,
    hostname=broker, client_id=client_id, clean_session=True)

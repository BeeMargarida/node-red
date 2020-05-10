import gc
import sys
import ujson
import uasyncio as asyncio
mqtt_client = None
nodes_id = ["8ee2fa766a7af"]
input_topics = ["input"]
output_topics = ["topic0_node"]

import ubinascii
input_topics_8ee2fa766a7af = ["input"]
output_topics_8ee2fa766a7af = ["topic0_node"]
node_datatype_8ee2fa766a7af = "auto"
node_qos_8ee2fa766a7af = 2

def on_input_8ee2fa766a7af(topic, msg, retained):
    if node_datatype_8ee2fa766a7af == "base64":
        msg = str(ubinascii.b2a_base64(msg))
    elif node_datatype_8ee2fa766a7af == "utf8":
        msg = msg.encode("utf-8")
    elif node_datatype_8ee2fa766a7af == "json":
        try:
            msg = ujson.loads(str(msg))
        except:
            print("Not a JSON")
    msg = dict(
        topic=topic,
        payload=msg,
        qos=node_qos_8ee2fa766a7af,
        retain=retained
    )
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(ujson.dumps(msg), output_topics_8ee2fa766a7af))

def on_input(topic, msg, retained):
    topic = topic.decode()
    if topic in input_topics_8ee2fa766a7af:
        on_input_8ee2fa766a7af(topic, msg, retained)

async def conn_han(client):
    for input_topic in input_topics:
        await client.subscribe(input_topic, 1)

async def on_output(msg, output):
    for output_topic in output:
        await mqtt_client.publish(output_topic, msg, qos = 1)

def stop():
    for id in nodes_id:
        func_name = "stop_" + id
        if func_name in globals():
            getattr(sys.modules[__name__], func_name)()

async def exec(mqtt_c):
    global mqtt_client
    mqtt_client = mqtt_c
    for id in nodes_id:
        func_name = "exec_" + id
        if func_name in globals():
            getattr(sys.modules[__name__], func_name)()
    return

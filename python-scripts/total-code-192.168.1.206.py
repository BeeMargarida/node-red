import gc
gc.collect()
import sys
gc.collect()
import ujson
gc.collect()
import uasyncio as asyncio
gc.collect()
mqtt_client = None
nodes_id = ["77f4640ac86d5c"]
input_topics = ["topic1_node"]
output_topics = []

input_topics_77f4640ac86d5c = ["topic1_node"]
output_topics_77f4640ac86d5c = ["results"]

def on_input_77f4640ac86d5c(topic, msg, retained):
    print(msg)            
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(msg, output_topics_77f4640ac86d5c))

def on_input(topic, msg, retained):
    topic = topic.decode()
    if topic in input_topics_77f4640ac86d5c:
        on_input_77f4640ac86d5c(topic, msg, retained)

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

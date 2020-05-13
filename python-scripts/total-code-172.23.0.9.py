import gc
import sys
import ujson
import uasyncio as asyncio
mqtt_client = None
nodes_id = ["3d8abc0fac4bec"]
input_topics = ["topic1_node","topic3_node"]
output_topics = ["topic4_node"]

input_topics_3d8abc0fac4bec = ["topic1_node","topic3_node"]
output_topics_3d8abc0fac4bec = ["topic4_node"]
nr_inputs_3d8abc0fac4bec = 2
property_3d8abc0fac4bec = "payload"
inputs_3d8abc0fac4bec = []
topics_3d8abc0fac4bec = []

def get_property_value_3d8abc0fac4bec(msg):
    properties = property_3d8abc0fac4bec.split(".")
    payload = ujson.loads(msg)

    for property in properties:
        try:
            if payload[property]:
                payload = payload[property]
            else:
                print("3d8abc0fac4bec: Property not found")
                break
        except:
            print("3d8abc0fac4bec: Msg is not an object")
            break

    return payload

def on_input_3d8abc0fac4bec(topic, msg, retained):
    global inputs_3d8abc0fac4bec
    global topics_3d8abc0fac4bec

    if not topic in topics_3d8abc0fac4bec:
        topics_3d8abc0fac4bec.append(topic)
        msg = get_property_value_3d8abc0fac4bec(msg)
        if (msg == 'True') or (msg == 'true'):
            inputs_3d8abc0fac4bec.append(True)
        elif (msg == 'False') or (msg == 'false'):
            inputs_3d8abc0fac4bec.append(False)
    
    if len(topics_3d8abc0fac4bec) == nr_inputs_3d8abc0fac4bec:
        result = True
        for entry in inputs_3d8abc0fac4bec:
            result = result and entry
        res = dict(
            payload=result
        )
        loop = asyncio.get_event_loop()
        loop.create_task(on_output(ujson.dumps(res), output_topics_3d8abc0fac4bec))
        inputs_3d8abc0fac4bec = []
        topics_3d8abc0fac4bec = []
    
    return

def on_input(topic, msg, retained):
    topic = topic.decode()
    if topic in input_topics_3d8abc0fac4bec:
        on_input_3d8abc0fac4bec(topic, msg, retained)

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

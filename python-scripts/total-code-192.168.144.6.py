import gc
import sys
import ujson
import uasyncio as asyncio
mqtt_client = None
nodes_id = ["aa22be326ee84"]
input_topics = ["topic2_node","topic4_node"]
output_topics = ["topic6_node"]

input_topics_aa22be326ee84 = ["topic2_node","topic4_node"]
output_topics_aa22be326ee84 = ["topic6_node"]
nr_inputs_aa22be326ee84 = 2
property_aa22be326ee84 = "payload"
inputs_aa22be326ee84 = []
topics_aa22be326ee84 = []

def get_property_value_aa22be326ee84(msg):
    properties = property_aa22be326ee84.split(".")
    payload = ujson.loads(msg)

    for property in properties:
        try:
            if payload[property]:
                payload = payload[property]
            else:
                print("aa22be326ee84: Property not found")
                break
        except:
            print("aa22be326ee84: Msg is not an object")
            break

    return payload

def on_input_aa22be326ee84(topic, msg, retained):
    global inputs_aa22be326ee84
    global topics_aa22be326ee84

    if not topic in topics_aa22be326ee84:
        topics_aa22be326ee84.append(topic)
        msg = get_property_value_aa22be326ee84(msg)
        if (msg == 'True') or (msg == 'true'):
            inputs_aa22be326ee84.append(True)
        elif (msg == 'False') or (msg == 'false'):
            inputs_aa22be326ee84.append(False)
    
    if len(topics_aa22be326ee84) == nr_inputs_aa22be326ee84:
        result = True
        for entry in inputs_aa22be326ee84:
            result = result and entry
        res = dict(
            payload=result
        )
        loop = asyncio.get_event_loop()
        loop.create_task(on_output(ujson.dumps(res), output_topics_aa22be326ee84))
        inputs_aa22be326ee84 = []
        topics_aa22be326ee84 = []
    
    return

def on_input(topic, msg, retained):
    topic = topic.decode()
    if topic in input_topics_aa22be326ee84:
        on_input_aa22be326ee84(topic, msg, retained)

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

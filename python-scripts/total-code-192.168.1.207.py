import gc
gc.collect()
import sys
gc.collect()
import ujson
gc.collect()
import uasyncio as asyncio
gc.collect()
mqtt_client = None
nodes_id = ["36bf026e0eb14e","da78822a07bc5"]
input_topics = ["topic0_node","topic2_node","topic6699dc48540db4_node_sub"]
output_topics = ["topic4_node","topic2_node","topic3_node"]

input_topics_36bf026e0eb14e = ["topic0_node","topic2_node"]
output_topics_36bf026e0eb14e = ["topic4_node"]
nr_inputs_36bf026e0eb14e = 2
property_36bf026e0eb14e = "payload"
inputs_36bf026e0eb14e = []
topics_36bf026e0eb14e = []

def get_property_value_36bf026e0eb14e(msg):
    properties = property_36bf026e0eb14e.split(".")
    payload = ujson.loads(msg)

    for property in properties:
        try:
            if payload[property]:
                payload = payload[property]
            else:
                print("36bf026e0eb14e: Property not found")
                break
        except:
            print("36bf026e0eb14e: Msg is not an object")
            break

    return payload

def on_input_36bf026e0eb14e(topic, msg, retained):
    global inputs_36bf026e0eb14e
    global topics_36bf026e0eb14e

    if not topic in topics_36bf026e0eb14e:
        topics_36bf026e0eb14e.append(topic)
        msg = get_property_value_36bf026e0eb14e(msg)
        if (msg == 'True') or (msg == 'true'):
            inputs_36bf026e0eb14e.append(True)
        elif (msg == 'False') or (msg == 'false'):
            inputs_36bf026e0eb14e.append(False)
    
    if len(topics_36bf026e0eb14e) == nr_inputs_36bf026e0eb14e:
        result = True
        for entry in inputs_36bf026e0eb14e:
            result = result and entry
        res = dict(
            payload=result
        )
        loop = asyncio.get_event_loop()
        loop.create_task(on_output(ujson.dumps(res), output_topics_36bf026e0eb14e))
        inputs_36bf026e0eb14e = []
        topics_36bf026e0eb14e = []
    
    return

input_topics_da78822a07bc5 = ["topic6699dc48540db4_node_sub"]
output_topics_da78822a07bc5 = ["topic2_node","topic3_node"]
property_da78822a07bc5 = "payload.temperature"

def if_rule_da78822a07bc5_0(a, b = 15):
    a = int(a)
    return a > b
def if_function_da78822a07bc5(a):
    res = if_rule_da78822a07bc5_0(a)
    return '%s' % res

def get_property_value_da78822a07bc5(msg):
    properties = property_da78822a07bc5.split(".")
    payload = ujson.loads(msg)

    for property in properties:
        try:
            if payload[property]:
                payload = payload[property]
            else:
                break
        except:
            break
    return payload

def on_input_da78822a07bc5(topic, msg, retained):
    msg = get_property_value_da78822a07bc5(msg)
    res = if_function_da78822a07bc5(msg)
    res = dict(
        payload=res
    )
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(ujson.dumps(res), output_topics_da78822a07bc5))
    return

def on_input(topic, msg, retained):
    topic = topic.decode()
    if topic in input_topics_36bf026e0eb14e:
        on_input_36bf026e0eb14e(topic, msg, retained)
    elif topic in input_topics_da78822a07bc5:
        on_input_da78822a07bc5(topic, msg, retained)

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

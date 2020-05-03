import gc
gc.collect()
import sys
gc.collect()
import ujson
gc.collect()
import uasyncio as asyncio
gc.collect()
mqtt_client = None
nodes_id = ["265365835d8e8a","a21867c32cc058"]
input_topics = ["topic233cfe16d92bc2_node_sub","topica505652ba855a_node_sub"]
output_topics = ["topic1_node","topic2_node"]

input_topics_265365835d8e8a = ["topic233cfe16d92bc2_node_sub"]
output_topics_265365835d8e8a = ["topic1_node"]
property_265365835d8e8a = "payload.temperature"

def if_rule_265365835d8e8a_0(a, b = 20):
    a = int(a)
    return a >= b
def if_function_265365835d8e8a(a):
    res = if_rule_265365835d8e8a_0(a)
    return '%s' % res

def get_property_value_265365835d8e8a(msg):
    properties = property_265365835d8e8a.split(".")
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

def on_input_265365835d8e8a(topic, msg, retained):
    msg = get_property_value_265365835d8e8a(msg)
    res = if_function_265365835d8e8a(msg)
    res = dict(
        payload=res
    )
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(ujson.dumps(res), output_topics_265365835d8e8a))
    return

input_topics_a21867c32cc058 = ["topica505652ba855a_node_sub"]
output_topics_a21867c32cc058 = ["topic2_node"]
property_a21867c32cc058 = "payload.humidity"

def if_rule_a21867c32cc058_0(a, b = 20):
    a = int(a)
    return a >= b
def if_function_a21867c32cc058(a):
    res = if_rule_a21867c32cc058_0(a)
    return '%s' % res

def get_property_value_a21867c32cc058(msg):
    properties = property_a21867c32cc058.split(".")
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

def on_input_a21867c32cc058(topic, msg, retained):
    msg = get_property_value_a21867c32cc058(msg)
    res = if_function_a21867c32cc058(msg)
    res = dict(
        payload=res
    )
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(ujson.dumps(res), output_topics_a21867c32cc058))
    return

def on_input(topic, msg, retained):
    topic = topic.decode()
    if topic in input_topics_265365835d8e8a:
        on_input_265365835d8e8a(topic, msg, retained)
    elif topic in input_topics_a21867c32cc058:
        on_input_a21867c32cc058(topic, msg, retained)

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

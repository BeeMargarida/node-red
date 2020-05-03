import gc
gc.collect()
import sys
gc.collect()
import ujson
gc.collect()
import uasyncio as asyncio
gc.collect()
mqtt_client = None
nodes_id = ["7156e3bb656294"]
input_topics = ["topic0_node"]
output_topics = ["topic1_node"]

input_topics_7156e3bb656294 = ["topic0_node"]
output_topics_7156e3bb656294 = ["topic1_node"]
property_7156e3bb656294 = "payload.temperature"

def if_rule_7156e3bb656294_0(a, b = 20):
    a = int(a)
    return a >= b
def if_function_7156e3bb656294(a):
    res = if_rule_7156e3bb656294_0(a)
    return '%s' % res

def get_property_value_7156e3bb656294(msg):
    properties = property_7156e3bb656294.split(".")
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

def on_input_7156e3bb656294(topic, msg, retained):
    msg = get_property_value_7156e3bb656294(msg)
    res = if_function_7156e3bb656294(msg)
    res = dict(
        payload=res
    )
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(ujson.dumps(res), output_topics_7156e3bb656294))
    return

def on_input(topic, msg, retained):
    topic = topic.decode()
    if topic in input_topics_7156e3bb656294:
        on_input_7156e3bb656294(topic, msg, retained)

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

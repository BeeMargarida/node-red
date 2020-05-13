import gc
import sys
import ujson
import uasyncio as asyncio
mqtt_client = None
nodes_id = ["ef4575edbeefe8"]
input_topics = ["topic2_node"]
output_topics = ["topic3_node"]

input_topics_ef4575edbeefe8 = ["topic2_node"]
output_topics_ef4575edbeefe8 = ["topic3_node"]
property_ef4575edbeefe8 = "payload.temperature"

def if_rule_ef4575edbeefe8_0(a, b = 15):
    a = int(a)
    return a >= b
def if_function_ef4575edbeefe8(a):
    res = if_rule_ef4575edbeefe8_0(a)
    return '%s' % res

def get_property_value_ef4575edbeefe8(msg):
    properties = property_ef4575edbeefe8.split(".")
    payload = ujson.loads(msg)
    payload = dict(payload=payload)

    for property in properties:
        try:
            if payload[property]:
                payload = payload[property]
            else:
                break
        except:
            break
    return payload

def on_input_ef4575edbeefe8(topic, msg, retained):
    print(topic)
    print(msg)
    msg = get_property_value_ef4575edbeefe8(msg)
    res = if_function_ef4575edbeefe8(msg)
    res = dict(
        payload=res
    )
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(ujson.dumps(res), output_topics_ef4575edbeefe8))
    return

def on_input(topic, msg, retained):
    topic = topic.decode()
    if topic in input_topics_ef4575edbeefe8:
        on_input_ef4575edbeefe8(topic, msg, retained)

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

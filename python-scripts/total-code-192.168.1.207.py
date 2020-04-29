import gc
gc.collect()
import sys
gc.collect()
import ujson
gc.collect()
import uasyncio as asyncio
gc.collect()
mqtt_client = None
nodes_id = ["292fca1ca69586","3a4475aaf6d8aa"]
input_topics = ["topic0_node","topic1_node"]
output_topics = ["topic2_node","topic3_node","topic4_node","topic5_node"]

input_topics_292fca1ca69586 = ["topic0_node"]
output_topics_292fca1ca69586 = ["topic2_node","topic3_node"]
property_292fca1ca69586 = "payload.temperature"

def if_rule_292fca1ca69586_0(a, b = 20):
    a = int(a)
    return a >= b
def if_function_292fca1ca69586(a):
    res = if_rule_292fca1ca69586_0(a)
    return '%s' % res

def get_property_value_292fca1ca69586(msg):
    properties = property_292fca1ca69586.split(".")
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

def on_input_292fca1ca69586(topic, msg, retained):
    msg = get_property_value_292fca1ca69586(msg)
    res = if_function_292fca1ca69586(msg)
    res = dict(
        payload=res
    )
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(ujson.dumps(res), output_topics_292fca1ca69586))
    return

input_topics_3a4475aaf6d8aa = ["topic1_node"]
output_topics_3a4475aaf6d8aa = ["topic4_node","topic5_node"]
property_3a4475aaf6d8aa = "payload.humidity"

def if_rule_3a4475aaf6d8aa_0(a, b = 20):
    a = int(a)
    return a >= b
def if_function_3a4475aaf6d8aa(a):
    res = if_rule_3a4475aaf6d8aa_0(a)
    return '%s' % res

def get_property_value_3a4475aaf6d8aa(msg):
    properties = property_3a4475aaf6d8aa.split(".")
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

def on_input_3a4475aaf6d8aa(topic, msg, retained):
    msg = get_property_value_3a4475aaf6d8aa(msg)
    res = if_function_3a4475aaf6d8aa(msg)
    res = dict(
        payload=res
    )
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(ujson.dumps(res), output_topics_3a4475aaf6d8aa))
    return

def on_input(topic, msg, retained):
    topic = topic.decode()
    if topic in input_topics_292fca1ca69586:
        on_input_292fca1ca69586(topic, msg, retained)
    elif topic in input_topics_3a4475aaf6d8aa:
        on_input_3a4475aaf6d8aa(topic, msg, retained)


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

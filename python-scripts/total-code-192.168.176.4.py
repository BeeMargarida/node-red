import gc
import sys
import ujson
import uasyncio as asyncio
mqtt_client = None
nodes_id = ["9cbe20c63332a"]
input_topics = ["topic0_node"]
output_topics = ["topic2_node","topic3_node"]

input_topics_9cbe20c63332a = ["topic0_node"]
output_topics_9cbe20c63332a = ["topic2_node","topic3_node"]
property_9cbe20c63332a = "payload.temperature"

def if_rule_9cbe20c63332a_0(a, b = 15):
    a = int(a)
    return a >= b
def if_function_9cbe20c63332a(a):
    res = if_rule_9cbe20c63332a_0(a)
    return '%s' % res

def get_property_value_9cbe20c63332a(msg):
    properties = property_9cbe20c63332a.split(".")
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

def on_input_9cbe20c63332a(topic, msg, retained):
    print(topic)
    print(msg)
    msg = get_property_value_9cbe20c63332a(msg)
    res = if_function_9cbe20c63332a(msg)
    res = dict(
        payload=res
    )
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(ujson.dumps(res), output_topics_9cbe20c63332a))
    return

def on_input(topic, msg, retained):
    topic = topic.decode()
    if topic in input_topics_9cbe20c63332a:
        on_input_9cbe20c63332a(topic, msg, retained)

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

import gc
import sys
import ujson
import uasyncio as asyncio
mqtt_client = None
nodes_id = ["a4327ad5b4ba8"]
input_topics = ["topic1_node"]
output_topics = ["topic4_node","topic5_node"]

input_topics_a4327ad5b4ba8 = ["topic1_node"]
output_topics_a4327ad5b4ba8 = ["topic4_node","topic5_node"]
property_a4327ad5b4ba8 = "payload.temperature"

def if_rule_a4327ad5b4ba8_0(a, b = 15):
    a = int(a)
    return a >= b
def if_function_a4327ad5b4ba8(a):
    res = if_rule_a4327ad5b4ba8_0(a)
    return '%s' % res

def get_property_value_a4327ad5b4ba8(msg):
    properties = property_a4327ad5b4ba8.split(".")
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

def on_input_a4327ad5b4ba8(topic, msg, retained):
    print(topic)
    print(msg)
    msg = get_property_value_a4327ad5b4ba8(msg)
    res = if_function_a4327ad5b4ba8(msg)
    res = dict(
        payload=res
    )
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(ujson.dumps(res), output_topics_a4327ad5b4ba8))
    return

def on_input(topic, msg, retained):
    topic = topic.decode()
    if topic in input_topics_a4327ad5b4ba8:
        on_input_a4327ad5b4ba8(topic, msg, retained)

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

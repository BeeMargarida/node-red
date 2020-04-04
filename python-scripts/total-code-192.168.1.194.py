import sys
import ujson
import uasyncio as asyncio
mqtt_client = None
nodes_id = ["b256e7e71a361"]
input_topics = ["topic1_node"]
output_topics = ["topic0_node"]

input_topics_b256e7e71a361 = ["topic1_node"]
output_topics_b256e7e71a361 = ["topic0_node"]
property_b256e7e71a361 = "payload.temperature"

def if_rule_b256e7e71a361_0(a, b = 40):
    a = int(a)
    return a >= b
def if_function_b256e7e71a361(a):
    res = if_rule_b256e7e71a361_0(a)
    return '%s' % res

def get_property_value_b256e7e71a361(msg):
    properties = property_b256e7e71a361.split(".")
    payload = ujson.loads(msg)

    for property in properties:
        try:
            if payload[property]:
                payload = payload[property]
            else:
                print("b256e7e71a361: Property not found")
                break
        except:
            print("b256e7e71a361: Msg is not an object")
            break

    return payload

def on_input_b256e7e71a361(topic, msg, retained):
    msg = get_property_value_b256e7e71a361(msg)
    res = if_function_b256e7e71a361(msg)

    # Create task to publish to output topics
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(res, output_topics_b256e7e71a361))


def on_input(topic, msg, retained):
    topic = topic.decode()
    if topic in input_topics_b256e7e71a361:
        on_input_b256e7e71a361(topic, msg, retained)


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

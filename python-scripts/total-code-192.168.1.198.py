import gc
gc.collect()
import sys
gc.collect()
import ujson
gc.collect()
import uasyncio as asyncio
gc.collect()
mqtt_client = None
nodes_id = ["b256e7e71a361","1d9c9acba02ea5","53d297eab1c518","fb0ed2cf7f5388","712536fb11f95"]
input_topics = ["topic2_node","topic4_node","topic1_node","topic5_node","topic9_node","topic8_node","orders"]
output_topics = ["topic0_node","topic1_node","topic5_node","topic6_node","topic7_node","topic8_node","topic9_node"]

input_topics_b256e7e71a361 = ["topic2_node"]
output_topics_b256e7e71a361 = ["topic0_node","topic1_node"]
property_b256e7e71a361 = "payload.temperature"

def if_rule_b256e7e71a361_0(a, b = 15):
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
                break
        except:
            break
    return payload

def on_input_b256e7e71a361(topic, msg, retained):
    msg = get_property_value_b256e7e71a361(msg)
    res = if_function_b256e7e71a361(msg)
    res = dict(
        payload=res
    )
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(ujson.dumps(res), output_topics_b256e7e71a361))
    return

input_topics_1d9c9acba02ea5 = ["topic4_node"]
output_topics_1d9c9acba02ea5 = ["topic5_node","topic6_node"]
property_1d9c9acba02ea5 = "payload.humidity"

def if_rule_1d9c9acba02ea5_0(a, b = 20):
    a = int(a)
    return a >= b
def if_function_1d9c9acba02ea5(a):
    res = if_rule_1d9c9acba02ea5_0(a)
    return '%s' % res

def get_property_value_1d9c9acba02ea5(msg):
    properties = property_1d9c9acba02ea5.split(".")
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

def on_input_1d9c9acba02ea5(topic, msg, retained):
    msg = get_property_value_1d9c9acba02ea5(msg)
    res = if_function_1d9c9acba02ea5(msg)
    res = dict(
        payload=res
    )
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(ujson.dumps(res), output_topics_1d9c9acba02ea5))
    return

input_topics_53d297eab1c518 = ["topic1_node","topic5_node","topic9_node"]
output_topics_53d297eab1c518 = ["topic7_node","topic8_node"]
nr_inputs_53d297eab1c518 = 3
property_53d297eab1c518 = "payload"
inputs_53d297eab1c518 = []
topics_53d297eab1c518 = []

def get_property_value_53d297eab1c518(msg):
    properties = property_53d297eab1c518.split(".")
    payload = ujson.loads(msg)

    for property in properties:
        try:
            if payload[property]:
                payload = payload[property]
            else:
                print("53d297eab1c518: Property not found")
                break
        except:
            print("53d297eab1c518: Msg is not an object")
            break

    return payload

def on_input_53d297eab1c518(topic, msg, retained):
    global inputs_53d297eab1c518
    global topics_53d297eab1c518

    if not topic in topics_53d297eab1c518:
        topics_53d297eab1c518.append(topic)
        msg = get_property_value_53d297eab1c518(msg)
        if (msg == 'True') or (msg == 'true'):
            inputs_53d297eab1c518.append(True)
        elif (msg == 'False') or (msg == 'false'):
            inputs_53d297eab1c518.append(False)
    
    if len(topics_53d297eab1c518) == nr_inputs_53d297eab1c518:
        result = True
        for entry in inputs_53d297eab1c518:
            result = result and entry
        res = dict(
            payload=result
        )
        loop = asyncio.get_event_loop()
        loop.create_task(on_output(ujson.dumps(res), output_topics_53d297eab1c518))
        inputs_53d297eab1c518 = []
        topics_53d297eab1c518 = []
    
    return

input_topics_fb0ed2cf7f5388 = ["topic8_node"]
output_topics_fb0ed2cf7f5388 = ["results"]

def on_input_fb0ed2cf7f5388(topic, msg, retained):
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(msg, output_topics_fb0ed2cf7f5388))

import ubinascii
gc.collect()
input_topics_712536fb11f95 = ["orders"]
output_topics_712536fb11f95 = ["topic9_node"]
node_datatype_712536fb11f95 = "auto"
node_qos_712536fb11f95 = 2

def on_input_712536fb11f95(topic, msg, retained):
    if node_datatype_712536fb11f95 == "base64":
        msg = str(ubinascii.b2a_base64(msg))
    elif node_datatype_712536fb11f95 == "utf8":
        msg = msg.encode("utf-8")
    elif node_datatype_712536fb11f95 == "json":
        try:
            msg = ujson.loads(str(msg))
        except:
            print("Not a JSON")
    msg = dict(
        topic=topic,
        payload=msg,
        qos=node_qos_712536fb11f95,
        retain=retained
    )
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(ujson.dumps(msg), output_topics_712536fb11f95))

def on_input(topic, msg, retained):
    topic = topic.decode()
    if topic in input_topics_b256e7e71a361:
        on_input_b256e7e71a361(topic, msg, retained)
    elif topic in input_topics_1d9c9acba02ea5:
        on_input_1d9c9acba02ea5(topic, msg, retained)
    elif topic in input_topics_53d297eab1c518:
        on_input_53d297eab1c518(topic, msg, retained)
    elif topic in input_topics_fb0ed2cf7f5388:
        on_input_fb0ed2cf7f5388(topic, msg, retained)
    elif topic in input_topics_712536fb11f95:
        on_input_712536fb11f95(topic, msg, retained)


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

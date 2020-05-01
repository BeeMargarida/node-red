import gc
gc.collect()
import sys
gc.collect()
import ujson
gc.collect()
import uasyncio as asyncio
gc.collect()
mqtt_client = None
nodes_id = ["deb83edafbfca","1031c1de65222e"]
input_topics = ["topic646e40b3392758_node_sub"]
output_topics = ["topic0_node","topic1_node","topic6699dc48540db4_node_sub"]

input_topics_deb83edafbfca = ["topic646e40b3392758_node_sub"]
output_topics_deb83edafbfca = ["topic0_node","topic1_node"]
property_deb83edafbfca = "payload.temperature"

def if_rule_deb83edafbfca_0(a, b = 15):
    a = int(a)
    return a > b
def if_function_deb83edafbfca(a):
    res = if_rule_deb83edafbfca_0(a)
    return '%s' % res

def get_property_value_deb83edafbfca(msg):
    properties = property_deb83edafbfca.split(".")
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

def on_input_deb83edafbfca(topic, msg, retained):
    msg = get_property_value_deb83edafbfca(msg)
    res = if_function_deb83edafbfca(msg)
    res = dict(
        payload=res
    )
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(ujson.dumps(res), output_topics_deb83edafbfca))
    return

import dht
gc.collect()
import machine
gc.collect()
output_topics_1031c1de65222e = ["topic6699dc48540db4_node_sub"]
pin_1031c1de65222e = 32
interval_1031c1de65222e = 5000
repeat_1031c1de65222e = True

reference_timer_workaround = []

def measure__1031c1de65222e(_):
    d = dht.DHT22(machine.Pin(pin_1031c1de65222e))
    d.measure()
    temperature = d.temperature()
    humidity = d.humidity()
    results = dict(
        payload=dict(
            temperature=temperature,
            humidity=humidity
        ) 
    )
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(ujson.dumps(results), output_topics_1031c1de65222e))

def stop_1031c1de65222e():
    for timer in reference_timer_workaround:
        timer.deinit()

def exec_1031c1de65222e():
    if repeat_1031c1de65222e:
        timer = machine.Timer(-1)    
        timer.init(period=interval_1031c1de65222e, mode=machine.Timer.PERIODIC, callback=measure__1031c1de65222e)
        reference_timer_workaround.append(timer)
    else: 
        measure(None)
    return

def on_input(topic, msg, retained):
    topic = topic.decode()
    if topic in input_topics_deb83edafbfca:
        on_input_deb83edafbfca(topic, msg, retained)

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

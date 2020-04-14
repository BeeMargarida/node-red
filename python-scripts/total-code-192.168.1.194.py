import gc
gc.collect()
import sys
gc.collect()
import ujson
gc.collect()
import uasyncio as asyncio
gc.collect()
mqtt_client = None
nodes_id = ["70e3b87f23e7e8","1d9c9acba02ea5","fb0ed2cf7f5388"]
input_topics = ["topic4_node","topic8_node"]
output_topics = ["topic2_node","topic3_node","topic5_node","topic6_node"]

import dht
gc.collect()
import machine
gc.collect()
output_topics_70e3b87f23e7e8 = ["topic2_node","topic3_node"]
pin_70e3b87f23e7e8 = 14
interval_70e3b87f23e7e8 = 5000
repeat_70e3b87f23e7e8 = True

reference_timer_workaround = []

def measure(_):
    d = dht.DHT22(machine.Pin(pin_70e3b87f23e7e8))
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
    loop.create_task(on_output(ujson.dumps(results), output_topics_70e3b87f23e7e8))

def stop_70e3b87f23e7e8():
    for timer in reference_timer_workaround:
        timer.deinit()

def exec_70e3b87f23e7e8():
    if repeat_70e3b87f23e7e8:
        timer = machine.Timer(-1)    
        timer.init(period=interval_70e3b87f23e7e8, mode=machine.Timer.PERIODIC, callback=measure)
        reference_timer_workaround.append(timer)
    else: 
        measure(None)
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

input_topics_fb0ed2cf7f5388 = ["topic8_node"]
output_topics_fb0ed2cf7f5388 = ["results"]

def on_input_fb0ed2cf7f5388(topic, msg, retained):
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(msg, output_topics_fb0ed2cf7f5388))

def on_input(topic, msg, retained):
    topic = topic.decode()
    if topic in input_topics_1d9c9acba02ea5:
        on_input_1d9c9acba02ea5(topic, msg, retained)
    elif topic in input_topics_fb0ed2cf7f5388:
        on_input_fb0ed2cf7f5388(topic, msg, retained)


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

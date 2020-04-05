import gc
gc.collect()
import sys
gc.collect()
import ujson
gc.collect()
import uasyncio as asyncio
gc.collect()
mqtt_client = None
nodes_id = ["5c254a58f2cab4","fb0ed2cf7f5388"]
input_topics = ["topic7_node"]
output_topics = ["topic3_node"]

import dht
gc.collect()
import machine
gc.collect()
output_topics_5c254a58f2cab4 = ["topic3_node"]
pin_5c254a58f2cab4 = 14
interval_5c254a58f2cab4 = 5000
repeat_5c254a58f2cab4 = True

reference_timer_workaround = []

def measure(_):
    d = dht.DHT22(machine.Pin(pin_5c254a58f2cab4))
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
    loop.create_task(on_output(ujson.dumps(results), output_topics_5c254a58f2cab4))

def stop_5c254a58f2cab4():
    for timer in reference_timer_workaround:
        timer.deinit()

def exec_5c254a58f2cab4():
    if repeat_5c254a58f2cab4:
        timer = machine.Timer(-1)    
        timer.init(period=interval_5c254a58f2cab4, mode=machine.Timer.PERIODIC, callback=measure)
        reference_timer_workaround.append(timer)
    else: 
        measure(None)
    return

input_topics_fb0ed2cf7f5388 = ["topic7_node"]
output_topics_fb0ed2cf7f5388 = ["results"]

def on_input_fb0ed2cf7f5388(topic, msg, retained):
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(msg, output_topics_fb0ed2cf7f5388))

def on_input(topic, msg, retained):
    topic = topic.decode()
    if topic in input_topics_fb0ed2cf7f5388:
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

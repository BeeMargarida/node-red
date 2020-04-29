import gc
gc.collect()
import sys
gc.collect()
import ujson
gc.collect()
import uasyncio as asyncio
gc.collect()
mqtt_client = None
nodes_id = ["3665c16bc9ba96","114281d7893ef6"]
input_topics = ["topic6_node"]
output_topics = ["topic1_node"]

import dht
gc.collect()
import machine
gc.collect()
output_topics_3665c16bc9ba96 = ["topic1_node"]
pin_3665c16bc9ba96 = 32
interval_3665c16bc9ba96 = 5000
repeat_3665c16bc9ba96 = True

reference_timer_workaround = []

def measure_3665c16bc9ba96(_):
    d = dht.DHT22(machine.Pin(pin_3665c16bc9ba96))
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
    loop.create_task(on_output(ujson.dumps(results), output_topics_3665c16bc9ba96))

def stop_3665c16bc9ba96():
    for timer in reference_timer_workaround:
        timer.deinit()

def exec_3665c16bc9ba96():
    if repeat_3665c16bc9ba96:
        timer = machine.Timer(-1)    
        timer.init(period=interval_3665c16bc9ba96, mode=machine.Timer.PERIODIC, callback=measure_3665c16bc9ba96)
        reference_timer_workaround.append(timer)
    else: 
        measure_3665c16bc9ba96(None)
    return

input_topics_114281d7893ef6 = ["topic6_node"]
output_topics_114281d7893ef6 = ["results"]

def on_input_114281d7893ef6(topic, msg, retained):
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(msg, output_topics_114281d7893ef6))

def on_input(topic, msg, retained):
    topic = topic.decode()
    if topic in input_topics_114281d7893ef6:
        on_input_114281d7893ef6(topic, msg, retained)


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

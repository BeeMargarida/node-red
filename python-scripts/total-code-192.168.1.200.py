import gc
gc.collect()
import sys
gc.collect()
import ujson
gc.collect()
import uasyncio as asyncio
gc.collect()
mqtt_client = None
nodes_id = ["ee3852f249b1a"]
input_topics = []
output_topics = ["topic0_node"]

import dht
gc.collect()
import machine
gc.collect()
output_topics_ee3852f249b1a = ["topic0_node"]
pin_ee3852f249b1a = 32
interval_ee3852f249b1a = 5000
repeat_ee3852f249b1a = True

reference_timer_workaround = []

def measure_ee3852f249b1a(_):
    d = dht.DHT22(machine.Pin(pin_ee3852f249b1a))
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
    loop.create_task(on_output(ujson.dumps(results), output_topics_ee3852f249b1a))

def stop_ee3852f249b1a():
    for timer in reference_timer_workaround:
        timer.deinit()

def exec_ee3852f249b1a():
    if repeat_ee3852f249b1a:
        timer = machine.Timer(-1)    
        timer.init(period=interval_ee3852f249b1a, mode=machine.Timer.PERIODIC, callback=measure_ee3852f249b1a)
        reference_timer_workaround.append(timer)
    else: 
        measure_ee3852f249b1a(None)
    return

def on_input(topic, msg, retained):
    topic = topic.decode()


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

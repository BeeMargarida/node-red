import gc
gc.collect()
import sys
gc.collect()
import ujson
gc.collect()
import uasyncio as asyncio
gc.collect()
mqtt_client = None
nodes_id = ["2033298f95c866"]
input_topics = []
output_topics = ["topic646e40b3392758_node_sub"]

import dht
gc.collect()
import machine
gc.collect()
output_topics_2033298f95c866 = ["topic646e40b3392758_node_sub"]
pin_2033298f95c866 = 32
interval_2033298f95c866 = 5000
repeat_2033298f95c866 = True

reference_timer_workaround = []

def measure__2033298f95c866(_):
    d = dht.DHT22(machine.Pin(pin_2033298f95c866))
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
    loop.create_task(on_output(ujson.dumps(results), output_topics_2033298f95c866))

def stop_2033298f95c866():
    for timer in reference_timer_workaround:
        timer.deinit()

def exec_2033298f95c866():
    if repeat_2033298f95c866:
        timer = machine.Timer(-1)    
        timer.init(period=interval_2033298f95c866, mode=machine.Timer.PERIODIC, callback=measure__2033298f95c866)
        reference_timer_workaround.append(timer)
    else: 
        measure(None)
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

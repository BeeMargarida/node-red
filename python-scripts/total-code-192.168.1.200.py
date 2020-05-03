import gc
gc.collect()
import sys
gc.collect()
import ujson
gc.collect()
import uasyncio as asyncio
gc.collect()
mqtt_client = None
nodes_id = ["a58d53e42067c"]
input_topics = []
output_topics = ["topic0_node"]

import dht
gc.collect()
import machine
gc.collect()
output_topics_a58d53e42067c = ["topic0_node"]
pin_a58d53e42067c = 32
interval_a58d53e42067c = 5000
repeat_a58d53e42067c = True

reference_timer_workaround = []

def measure_a58d53e42067c(_):
    d = dht.DHT22(machine.Pin(pin_a58d53e42067c))
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
    loop.create_task(on_output(ujson.dumps(results), output_topics_a58d53e42067c))

def stop_a58d53e42067c():
    for timer in reference_timer_workaround:
        timer.deinit()

def exec_a58d53e42067c():
    if repeat_a58d53e42067c:
        timer = machine.Timer(-1)    
        timer.init(period=interval_a58d53e42067c, mode=machine.Timer.PERIODIC, callback=measure_a58d53e42067c)
        reference_timer_workaround.append(timer)
    else: 
        measure_a58d53e42067c(None)
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

import gc
gc.collect()
import sys
gc.collect()
import ujson
gc.collect()
import uasyncio as asyncio
gc.collect()
mqtt_client = None
nodes_id = ["89e731aa94d7d","811d44e1d60768"]
input_topics = ["topic0_node"]
output_topics = ["topic233cfe16d92bc2_node_sub"]

input_topics_89e731aa94d7d = ["topic0_node"]
output_topics_89e731aa94d7d = ["results"]

def on_input_89e731aa94d7d(topic, msg, retained):
    print(msg)            
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(msg, output_topics_89e731aa94d7d))

import dht
gc.collect()
import machine
gc.collect()
output_topics_811d44e1d60768 = ["topic233cfe16d92bc2_node_sub"]
pin_811d44e1d60768 = 32
interval_811d44e1d60768 = 5000
repeat_811d44e1d60768 = True

reference_timer_workaround = []

def measure_811d44e1d60768(_):
    d = dht.DHT22(machine.Pin(pin_811d44e1d60768))
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
    loop.create_task(on_output(ujson.dumps(results), output_topics_811d44e1d60768))

def stop_811d44e1d60768():
    for timer in reference_timer_workaround:
        timer.deinit()

def exec_811d44e1d60768():
    if repeat_811d44e1d60768:
        timer = machine.Timer(-1)    
        timer.init(period=interval_811d44e1d60768, mode=machine.Timer.PERIODIC, callback=measure_811d44e1d60768)
        reference_timer_workaround.append(timer)
    else: 
        measure_811d44e1d60768(None)
    return

def on_input(topic, msg, retained):
    topic = topic.decode()
    if topic in input_topics_89e731aa94d7d:
        on_input_89e731aa94d7d(topic, msg, retained)

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

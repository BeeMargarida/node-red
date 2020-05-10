import gc
import sys
import ujson
import uasyncio as asyncio
mqtt_client = None
nodes_id = ["8d4b1449c8d91"]
input_topics = []
output_topics = ["topic1_node"]

import dht
import machine
import sys
output_topics_8d4b1449c8d91 = ["topic1_node"]
pin_8d4b1449c8d91 = 32
interval_8d4b1449c8d91 = 5000
repeat_8d4b1449c8d91 = True
stop_repeat_8d4b1449c8d91 = False

reference_timer_workaround = []

def measure_8d4b1449c8d91(_):
    pin = None
    if sys.platform != "linux":
        pin = machine.Pin(pin_8d4b1449c8d91)
    d = dht.DHT22(pin)
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
    loop.create_task(on_output(ujson.dumps(results), output_topics_8d4b1449c8d91))

def stop_8d4b1449c8d91():
    global stop_repeat_8d4b1449c8d91
    stop_repeat_8d4b1449c8d91 = True
    for timer in reference_timer_workaround:
        timer.deinit()

async def timer_exec_8d4b1449c8d91(callback, interval):
    if stop_repeat_8d4b1449c8d91:
        return
    callback(None)
    await asyncio.sleep_ms(interval)
    loop = asyncio.get_event_loop()
    loop.create_task(timer_exec_8d4b1449c8d91(callback, interval))

def exec_8d4b1449c8d91():
    if repeat_8d4b1449c8d91:
        if sys.platform != "linux":
            timer = machine.Timer(-1)    
            timer.init(period=interval_8d4b1449c8d91, mode=machine.Timer.PERIODIC, callback=measure_8d4b1449c8d91)
            reference_timer_workaround.append(timer)
        else:
            loop = asyncio.get_event_loop()
            loop.create_task(timer_exec_8d4b1449c8d91(measure_8d4b1449c8d91, interval_8d4b1449c8d91))
    else: 
        measure_8d4b1449c8d91(None)
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

import gc
import sys
import ujson
import uasyncio as asyncio
mqtt_client = None
nodes_id = ["973613f1ffaa"]
input_topics = []
output_topics = ["topic2_node"]

import dht
import machine
import sys
import utime
output_topics_973613f1ffaa = ["topic2_node"]
pin_973613f1ffaa = 32
interval_973613f1ffaa = 7000
repeat_973613f1ffaa = True
stop_repeat_973613f1ffaa = False
timer_task_973613f1ffaa = None

reference_timer_workaround = []

def measure_973613f1ffaa(_):
    pin = None
    if sys.platform != "linux":
        pin = machine.Pin(pin_973613f1ffaa)
    d = dht.DHT22(pin)
    d.measure()
    temperature = d.temperature()
    humidity = d.humidity()
    results = dict(
        temperature=temperature,
        humidity=humidity,
        _msgid=str(utime.ticks_ms())
    )
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(ujson.dumps(results), output_topics_973613f1ffaa))

def stop_973613f1ffaa():
    global stop_repeat_973613f1ffaa
    stop_repeat_973613f1ffaa = True
    if timer_task_973613f1ffaa:
        timer_task_973613f1ffaa.cancel()
    for timer in reference_timer_workaround:
        timer.deinit()

async def timer_exec_973613f1ffaa(callback, interval):
    global timer_task_973613f1ffaa
    if stop_repeat_973613f1ffaa:
        return
    callback(None)
    await asyncio.sleep_ms(interval)
    loop = asyncio.get_event_loop()
    timer_task_973613f1ffaa = loop.create_task(timer_exec_973613f1ffaa(callback, interval))

def exec_973613f1ffaa():
    if repeat_973613f1ffaa:
        if sys.platform != "linux":
            timer = machine.Timer(-1)    
            timer.init(period=interval_973613f1ffaa, mode=machine.Timer.PERIODIC, callback=measure_973613f1ffaa)
            reference_timer_workaround.append(timer)
        else:
            loop = asyncio.get_event_loop()
            print("starting")
            loop.create_task(timer_exec_973613f1ffaa(measure_973613f1ffaa, interval_973613f1ffaa))
    else: 
        measure_973613f1ffaa(None)
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

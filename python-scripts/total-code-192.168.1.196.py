import sys
import ujson
import uasyncio as asyncio
mqtt_client = None
nodes_id = ["70e3b87f23e7e8"]
input_topics = []
output_topics = ["topic1_node"]

import dht
import machine
output_topics_70e3b87f23e7e8 = ["topic1_node"]
pin_70e3b87f23e7e8 = 14
interval_70e3b87f23e7e8 = 5000
repeat_70e3b87f23e7e8 = False

# workaround for initialized timer not being
# referenced by pythoncode and deleted by gc
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
    encoded = ujson.dumps(results)
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(encoded, output_topics_70e3b87f23e7e8))


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

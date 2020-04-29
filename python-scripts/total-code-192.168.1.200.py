import gc
gc.collect()
import sys
gc.collect()
import ujson
gc.collect()
import uasyncio as asyncio
gc.collect()
mqtt_client = None
nodes_id = ["85734e52d37828","addb6c491803b"]
input_topics = ["topic2_node","topic4_node"]
output_topics = ["topic0_node","topic6_node","topic7_node"]

import dht
gc.collect()
import machine
gc.collect()
output_topics_85734e52d37828 = ["topic0_node"]
pin_85734e52d37828 = 32
interval_85734e52d37828 = 5000
repeat_85734e52d37828 = True

reference_timer_workaround = []

def measure_85734e52d37828(_):
    d = dht.DHT22(machine.Pin(pin_85734e52d37828))
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
    loop.create_task(on_output(ujson.dumps(results), output_topics_85734e52d37828))

def stop_85734e52d37828():
    for timer in reference_timer_workaround:
        timer.deinit()

def exec_85734e52d37828():
    if repeat_85734e52d37828:
        timer = machine.Timer(-1)    
        timer.init(period=interval_85734e52d37828, mode=machine.Timer.PERIODIC, callback=measure_85734e52d37828)
        reference_timer_workaround.append(timer)
    else: 
        measure_85734e52d37828(None)
    return

input_topics_addb6c491803b = ["topic2_node","topic4_node"]
output_topics_addb6c491803b = ["topic6_node","topic7_node"]
nr_inputs_addb6c491803b = 2
property_addb6c491803b = "payload"
inputs_addb6c491803b = []
topics_addb6c491803b = []

def get_property_value_addb6c491803b(msg):
    properties = property_addb6c491803b.split(".")
    payload = ujson.loads(msg)

    for property in properties:
        try:
            if payload[property]:
                payload = payload[property]
            else:
                print("addb6c491803b: Property not found")
                break
        except:
            print("addb6c491803b: Msg is not an object")
            break

    return payload

def on_input_addb6c491803b(topic, msg, retained):
    global inputs_addb6c491803b
    global topics_addb6c491803b

    if not topic in topics_addb6c491803b:
        topics_addb6c491803b.append(topic)
        msg = get_property_value_addb6c491803b(msg)
        if (msg == 'True') or (msg == 'true'):
            inputs_addb6c491803b.append(True)
        elif (msg == 'False') or (msg == 'false'):
            inputs_addb6c491803b.append(False)
    
    if len(topics_addb6c491803b) == nr_inputs_addb6c491803b:
        result = True
        for entry in inputs_addb6c491803b:
            result = result and entry
        res = dict(
            payload=result
        )
        loop = asyncio.get_event_loop()
        loop.create_task(on_output(ujson.dumps(res), output_topics_addb6c491803b))
        inputs_addb6c491803b = []
        topics_addb6c491803b = []
    
    return

def on_input(topic, msg, retained):
    topic = topic.decode()
    if topic in input_topics_addb6c491803b:
        on_input_addb6c491803b(topic, msg, retained)


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

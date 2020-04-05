import gc
gc.collect()
import sys
gc.collect()
import ujson
gc.collect()
import uasyncio as asyncio
gc.collect()
mqtt_client = None
nodes_id = ["70e3b87f23e7e8","53d297eab1c518"]
input_topics = ["topic1_node","topic4_node","topic8_node"]
output_topics = ["topic2_node","topic6_node","topic7_node"]

import dht
gc.collect()
import machine
gc.collect()
output_topics_70e3b87f23e7e8 = ["topic2_node"]
pin_70e3b87f23e7e8 = 14
interval_70e3b87f23e7e8 = 5000
repeat_70e3b87f23e7e8 = True

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
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(ujson.dumps(results), output_topics_70e3b87f23e7e8))

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

input_topics_53d297eab1c518 = ["topic1_node","topic4_node","topic8_node"]
output_topics_53d297eab1c518 = ["topic6_node","topic7_node"]
nr_inputs_53d297eab1c518 = 3
property_53d297eab1c518 = "payload"
inputs_53d297eab1c518 = []
topics_53d297eab1c518 = []

def get_property_value_53d297eab1c518(msg):
    properties = property_53d297eab1c518.split(".")
    payload = ujson.loads(msg)

    for property in properties:
        try:
            if payload[property]:
                payload = payload[property]
            else:
                print("53d297eab1c518: Property not found")
                break
        except:
            print("53d297eab1c518: Msg is not an object")
            break

    return payload

def on_input_53d297eab1c518(topic, msg, retained):
    global inputs_53d297eab1c518
    global topics_53d297eab1c518

    if not topic in topics_53d297eab1c518:
        topics_53d297eab1c518.append(topic)
        msg = get_property_value_53d297eab1c518(msg)
        if (msg == 'True') or (msg == 'true'):
            inputs_53d297eab1c518.append(True)
        elif (msg == 'False') or (msg == 'false'):
            inputs_53d297eab1c518.append(False)
    
    if len(topics_53d297eab1c518) == nr_inputs_53d297eab1c518:
        result = True
        for entry in inputs_53d297eab1c518:
            result = result and entry
        res = dict(
            payload=result
        )
        loop = asyncio.get_event_loop()
        loop.create_task(on_output(ujson.dumps(res), output_topics_53d297eab1c518))
        inputs_53d297eab1c518 = []
        topics_53d297eab1c518 = []
    
    return

def on_input(topic, msg, retained):
    topic = topic.decode()
    if topic in input_topics_53d297eab1c518:
        on_input_53d297eab1c518(topic, msg, retained)


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

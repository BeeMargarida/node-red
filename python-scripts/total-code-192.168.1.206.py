import gc
gc.collect()
import sys
gc.collect()
import ujson
gc.collect()
import uasyncio as asyncio
gc.collect()
mqtt_client = None
nodes_id = ["50b1607d098af","652c4f855f4e4"]
input_topics = ["topic1_node","topic2_node"]
output_topics = ["topic0_node","topica505652ba855a_node_sub"]

input_topics_50b1607d098af = ["topic1_node","topic2_node"]
output_topics_50b1607d098af = ["topic0_node"]
nr_inputs_50b1607d098af = 2
property_50b1607d098af = "payload"
inputs_50b1607d098af = []
topics_50b1607d098af = []

def get_property_value_50b1607d098af(msg):
    properties = property_50b1607d098af.split(".")
    payload = ujson.loads(msg)

    for property in properties:
        try:
            if payload[property]:
                payload = payload[property]
            else:
                print("50b1607d098af: Property not found")
                break
        except:
            print("50b1607d098af: Msg is not an object")
            break

    return payload

def on_input_50b1607d098af(topic, msg, retained):
    global inputs_50b1607d098af
    global topics_50b1607d098af

    if not topic in topics_50b1607d098af:
        topics_50b1607d098af.append(topic)
        msg = get_property_value_50b1607d098af(msg)
        if (msg == 'True') or (msg == 'true'):
            inputs_50b1607d098af.append(True)
        elif (msg == 'False') or (msg == 'false'):
            inputs_50b1607d098af.append(False)
    
    if len(topics_50b1607d098af) == nr_inputs_50b1607d098af:
        result = True
        for entry in inputs_50b1607d098af:
            result = result and entry
        res = dict(
            payload=result
        )
        print("AND: " + ujson.dumps(res))
        loop = asyncio.get_event_loop()
        loop.create_task(on_output(ujson.dumps(res), output_topics_50b1607d098af))
        inputs_50b1607d098af = []
        topics_50b1607d098af = []
    
    return

import dht
gc.collect()
import machine
gc.collect()
output_topics_652c4f855f4e4 = ["topica505652ba855a_node_sub"]
pin_652c4f855f4e4 = 32
interval_652c4f855f4e4 = 6000
repeat_652c4f855f4e4 = True

reference_timer_workaround = []

def measure_652c4f855f4e4(_):
    d = dht.DHT22(machine.Pin(pin_652c4f855f4e4))
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
    loop.create_task(on_output(ujson.dumps(results), output_topics_652c4f855f4e4))

def stop_652c4f855f4e4():
    for timer in reference_timer_workaround:
        timer.deinit()

def exec_652c4f855f4e4():
    if repeat_652c4f855f4e4:
        timer = machine.Timer(-1)    
        timer.init(period=interval_652c4f855f4e4, mode=machine.Timer.PERIODIC, callback=measure_652c4f855f4e4)
        reference_timer_workaround.append(timer)
    else: 
        measure_652c4f855f4e4(None)
    return

def on_input(topic, msg, retained):
    topic = topic.decode()
    if topic in input_topics_50b1607d098af:
        on_input_50b1607d098af(topic, msg, retained)

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

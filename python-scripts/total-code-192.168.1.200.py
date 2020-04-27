import gc
gc.collect()
import sys
gc.collect()
import ujson
gc.collect()
import uasyncio as asyncio
gc.collect()
mqtt_client = None
nodes_id = ["70e3b87f23e7e8","53d297eab1c518","b256e7e71a361","5c254a58f2cab4","53d297eab1c518","b256e7e71a361","70e3b87f23e7e8","5c254a58f2cab4","1d9c9acba02ea5","53d297eab1c518","fb0ed2cf7f5388"]
input_topics = ["topic1_node","topic5_node","topic2_node","topic4_node","topic8_node"]
output_topics = ["topic2_node","topic3_node","topic7_node","topic8_node","topic0_node","topic1_node","topic4_node","topic5_node","topic6_node"]

import dht
gc.collect()
import machine
gc.collect()
output_topics_70e3b87f23e7e8 = ["topic2_node","topic3_node"]
pin_70e3b87f23e7e8 = 32
interval_70e3b87f23e7e8 = 5000
repeat_70e3b87f23e7e8 = True

reference_timer_workaround = []

def measure__70e3b87f23e7e8(_):
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
        timer.init(period=interval_70e3b87f23e7e8, mode=machine.Timer.PERIODIC, callback=measure__70e3b87f23e7e8)
        reference_timer_workaround.append(timer)
    else: 
        measure(None)
    return

input_topics_53d297eab1c518 = ["topic1_node","topic5_node"]
output_topics_53d297eab1c518 = ["topic7_node","topic8_node"]
nr_inputs_53d297eab1c518 = 2
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

input_topics_b256e7e71a361 = ["topic2_node"]
output_topics_b256e7e71a361 = ["topic0_node","topic1_node"]
property_b256e7e71a361 = "payload.temperature"

def if_rule_b256e7e71a361_0(a, b = 15):
    a = int(a)
    return a >= b
def if_function_b256e7e71a361(a):
    res = if_rule_b256e7e71a361_0(a)
    return '%s' % res

def get_property_value_b256e7e71a361(msg):
    properties = property_b256e7e71a361.split(".")
    payload = ujson.loads(msg)

    for property in properties:
        try:
            if payload[property]:
                payload = payload[property]
            else:
                break
        except:
            break
    return payload

def on_input_b256e7e71a361(topic, msg, retained):
    msg = get_property_value_b256e7e71a361(msg)
    res = if_function_b256e7e71a361(msg)
    res = dict(
        payload=res
    )
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(ujson.dumps(res), output_topics_b256e7e71a361))
    return

import dht
gc.collect()
import machine
gc.collect()
output_topics_5c254a58f2cab4 = ["topic4_node"]
pin_5c254a58f2cab4 = 32
interval_5c254a58f2cab4 = 5000
repeat_5c254a58f2cab4 = True

reference_timer_workaround = []

def measure__5c254a58f2cab4(_):
    d = dht.DHT22(machine.Pin(pin_5c254a58f2cab4))
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
    loop.create_task(on_output(ujson.dumps(results), output_topics_5c254a58f2cab4))

def stop_5c254a58f2cab4():
    for timer in reference_timer_workaround:
        timer.deinit()

def exec_5c254a58f2cab4():
    if repeat_5c254a58f2cab4:
        timer = machine.Timer(-1)    
        timer.init(period=interval_5c254a58f2cab4, mode=machine.Timer.PERIODIC, callback=measure__5c254a58f2cab4)
        reference_timer_workaround.append(timer)
    else: 
        measure(None)
    return

input_topics_53d297eab1c518 = ["topic1_node","topic5_node"]
output_topics_53d297eab1c518 = ["topic7_node","topic8_node"]
nr_inputs_53d297eab1c518 = 2
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

input_topics_b256e7e71a361 = ["topic2_node"]
output_topics_b256e7e71a361 = ["topic0_node","topic1_node"]
property_b256e7e71a361 = "payload.temperature"

def if_rule_b256e7e71a361_0(a, b = 15):
    a = int(a)
    return a >= b
def if_function_b256e7e71a361(a):
    res = if_rule_b256e7e71a361_0(a)
    return '%s' % res

def get_property_value_b256e7e71a361(msg):
    properties = property_b256e7e71a361.split(".")
    payload = ujson.loads(msg)

    for property in properties:
        try:
            if payload[property]:
                payload = payload[property]
            else:
                break
        except:
            break
    return payload

def on_input_b256e7e71a361(topic, msg, retained):
    msg = get_property_value_b256e7e71a361(msg)
    res = if_function_b256e7e71a361(msg)
    res = dict(
        payload=res
    )
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(ujson.dumps(res), output_topics_b256e7e71a361))
    return

import dht
gc.collect()
import machine
gc.collect()
output_topics_70e3b87f23e7e8 = ["topic2_node","topic3_node"]
pin_70e3b87f23e7e8 = 32
interval_70e3b87f23e7e8 = 5000
repeat_70e3b87f23e7e8 = True

reference_timer_workaround = []

def measure__70e3b87f23e7e8(_):
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
        timer.init(period=interval_70e3b87f23e7e8, mode=machine.Timer.PERIODIC, callback=measure__70e3b87f23e7e8)
        reference_timer_workaround.append(timer)
    else: 
        measure(None)
    return

import dht
gc.collect()
import machine
gc.collect()
output_topics_5c254a58f2cab4 = ["topic4_node"]
pin_5c254a58f2cab4 = 32
interval_5c254a58f2cab4 = 5000
repeat_5c254a58f2cab4 = True

reference_timer_workaround = []

def measure__5c254a58f2cab4(_):
    d = dht.DHT22(machine.Pin(pin_5c254a58f2cab4))
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
    loop.create_task(on_output(ujson.dumps(results), output_topics_5c254a58f2cab4))

def stop_5c254a58f2cab4():
    for timer in reference_timer_workaround:
        timer.deinit()

def exec_5c254a58f2cab4():
    if repeat_5c254a58f2cab4:
        timer = machine.Timer(-1)    
        timer.init(period=interval_5c254a58f2cab4, mode=machine.Timer.PERIODIC, callback=measure__5c254a58f2cab4)
        reference_timer_workaround.append(timer)
    else: 
        measure(None)
    return

input_topics_1d9c9acba02ea5 = ["topic4_node"]
output_topics_1d9c9acba02ea5 = ["topic5_node","topic6_node"]
property_1d9c9acba02ea5 = "payload.humidity"

def if_rule_1d9c9acba02ea5_0(a, b = 20):
    a = int(a)
    return a >= b
def if_function_1d9c9acba02ea5(a):
    res = if_rule_1d9c9acba02ea5_0(a)
    return '%s' % res

def get_property_value_1d9c9acba02ea5(msg):
    properties = property_1d9c9acba02ea5.split(".")
    payload = ujson.loads(msg)

    for property in properties:
        try:
            if payload[property]:
                payload = payload[property]
            else:
                break
        except:
            break
    return payload

def on_input_1d9c9acba02ea5(topic, msg, retained):
    msg = get_property_value_1d9c9acba02ea5(msg)
    res = if_function_1d9c9acba02ea5(msg)
    res = dict(
        payload=res
    )
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(ujson.dumps(res), output_topics_1d9c9acba02ea5))
    return

input_topics_53d297eab1c518 = ["topic1_node","topic5_node"]
output_topics_53d297eab1c518 = ["topic7_node","topic8_node"]
nr_inputs_53d297eab1c518 = 2
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

input_topics_fb0ed2cf7f5388 = ["topic8_node"]
output_topics_fb0ed2cf7f5388 = ["results"]

def on_input_fb0ed2cf7f5388(topic, msg, retained):
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(msg, output_topics_fb0ed2cf7f5388))

def on_input(topic, msg, retained):
    topic = topic.decode()
    if topic in input_topics_53d297eab1c518:
        on_input_53d297eab1c518(topic, msg, retained)
    elif topic in input_topics_b256e7e71a361:
        on_input_b256e7e71a361(topic, msg, retained)
    elif topic in input_topics_53d297eab1c518:
        on_input_53d297eab1c518(topic, msg, retained)
    elif topic in input_topics_b256e7e71a361:
        on_input_b256e7e71a361(topic, msg, retained)
    elif topic in input_topics_1d9c9acba02ea5:
        on_input_1d9c9acba02ea5(topic, msg, retained)
    elif topic in input_topics_53d297eab1c518:
        on_input_53d297eab1c518(topic, msg, retained)
    elif topic in input_topics_fb0ed2cf7f5388:
        on_input_fb0ed2cf7f5388(topic, msg, retained)


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

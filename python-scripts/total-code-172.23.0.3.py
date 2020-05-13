import gc
import sys
import ujson
import uasyncio as asyncio
mqtt_client = None
nodes_id = ["16c920333c8ed","79c5d6a8b876a","ef4575edbeefe8","3d8abc0fac4bec"]
input_topics = ["topic0_node","topic2_node","topic1_node","topic3_node"]
output_topics = ["topic0_node","topic1_node","topic3_node","topic4_node"]

import dht
import machine
import sys
import utime
output_topics_16c920333c8ed = ["topic0_node"]
pin_16c920333c8ed = 32
interval_16c920333c8ed = 7000
repeat_16c920333c8ed = True
stop_repeat_16c920333c8ed = False
timer_task_16c920333c8ed = None

reference_timer_workaround = []

def measure_16c920333c8ed(_):
    pin = None
    if sys.platform != "linux":
        pin = machine.Pin(pin_16c920333c8ed)
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
    loop.create_task(on_output(ujson.dumps(results), output_topics_16c920333c8ed))

def stop_16c920333c8ed():
    global stop_repeat_16c920333c8ed
    stop_repeat_16c920333c8ed = True
    if timer_task_16c920333c8ed:
        timer_task_16c920333c8ed.cancel()
    for timer in reference_timer_workaround:
        timer.deinit()

async def timer_exec_16c920333c8ed(callback, interval):
    global timer_task_16c920333c8ed
    if stop_repeat_16c920333c8ed:
        return
    callback(None)
    await asyncio.sleep_ms(interval)
    loop = asyncio.get_event_loop()
    timer_task_16c920333c8ed = loop.create_task(timer_exec_16c920333c8ed(callback, interval))

def exec_16c920333c8ed():
    if repeat_16c920333c8ed:
        if sys.platform != "linux":
            timer = machine.Timer(-1)    
            timer.init(period=interval_16c920333c8ed, mode=machine.Timer.PERIODIC, callback=measure_16c920333c8ed)
            reference_timer_workaround.append(timer)
        else:
            loop = asyncio.get_event_loop()
            print("starting")
            loop.create_task(timer_exec_16c920333c8ed(measure_16c920333c8ed, interval_16c920333c8ed))
    else: 
        measure_16c920333c8ed(None)
    return

input_topics_79c5d6a8b876a = ["topic0_node"]
output_topics_79c5d6a8b876a = ["topic1_node"]
property_79c5d6a8b876a = "payload.temperature"

def if_rule_79c5d6a8b876a_0(a, b = 15):
    a = int(a)
    return a >= b
def if_function_79c5d6a8b876a(a):
    res = if_rule_79c5d6a8b876a_0(a)
    return '%s' % res

def get_property_value_79c5d6a8b876a(msg):
    properties = property_79c5d6a8b876a.split(".")
    payload = ujson.loads(msg)
    payload = dict(payload=payload)

    for property in properties:
        try:
            if payload[property]:
                payload = payload[property]
            else:
                break
        except:
            break
    return payload

def on_input_79c5d6a8b876a(topic, msg, retained):
    print(topic)
    print(msg)
    msg = get_property_value_79c5d6a8b876a(msg)
    res = if_function_79c5d6a8b876a(msg)
    res = dict(
        payload=res
    )
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(ujson.dumps(res), output_topics_79c5d6a8b876a))
    return

input_topics_ef4575edbeefe8 = ["topic2_node"]
output_topics_ef4575edbeefe8 = ["topic3_node"]
property_ef4575edbeefe8 = "payload.temperature"

def if_rule_ef4575edbeefe8_0(a, b = 15):
    a = int(a)
    return a >= b
def if_function_ef4575edbeefe8(a):
    res = if_rule_ef4575edbeefe8_0(a)
    return '%s' % res

def get_property_value_ef4575edbeefe8(msg):
    properties = property_ef4575edbeefe8.split(".")
    payload = ujson.loads(msg)
    payload = dict(payload=payload)

    for property in properties:
        try:
            if payload[property]:
                payload = payload[property]
            else:
                break
        except:
            break
    return payload

def on_input_ef4575edbeefe8(topic, msg, retained):
    print(topic)
    print(msg)
    msg = get_property_value_ef4575edbeefe8(msg)
    res = if_function_ef4575edbeefe8(msg)
    res = dict(
        payload=res
    )
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(ujson.dumps(res), output_topics_ef4575edbeefe8))
    return

input_topics_3d8abc0fac4bec = ["topic1_node","topic3_node"]
output_topics_3d8abc0fac4bec = ["topic4_node"]
nr_inputs_3d8abc0fac4bec = 2
property_3d8abc0fac4bec = "payload"
inputs_3d8abc0fac4bec = []
topics_3d8abc0fac4bec = []

def get_property_value_3d8abc0fac4bec(msg):
    properties = property_3d8abc0fac4bec.split(".")
    payload = ujson.loads(msg)

    for property in properties:
        try:
            if payload[property]:
                payload = payload[property]
            else:
                print("3d8abc0fac4bec: Property not found")
                break
        except:
            print("3d8abc0fac4bec: Msg is not an object")
            break

    return payload

def on_input_3d8abc0fac4bec(topic, msg, retained):
    global inputs_3d8abc0fac4bec
    global topics_3d8abc0fac4bec

    if not topic in topics_3d8abc0fac4bec:
        topics_3d8abc0fac4bec.append(topic)
        msg = get_property_value_3d8abc0fac4bec(msg)
        if (msg == 'True') or (msg == 'true'):
            inputs_3d8abc0fac4bec.append(True)
        elif (msg == 'False') or (msg == 'false'):
            inputs_3d8abc0fac4bec.append(False)
    
    if len(topics_3d8abc0fac4bec) == nr_inputs_3d8abc0fac4bec:
        result = True
        for entry in inputs_3d8abc0fac4bec:
            result = result and entry
        res = dict(
            payload=result
        )
        loop = asyncio.get_event_loop()
        loop.create_task(on_output(ujson.dumps(res), output_topics_3d8abc0fac4bec))
        inputs_3d8abc0fac4bec = []
        topics_3d8abc0fac4bec = []
    
    return

def on_input(topic, msg, retained):
    topic = topic.decode()
    if topic in input_topics_79c5d6a8b876a:
        on_input_79c5d6a8b876a(topic, msg, retained)
    elif topic in input_topics_ef4575edbeefe8:
        on_input_ef4575edbeefe8(topic, msg, retained)
    elif topic in input_topics_3d8abc0fac4bec:
        on_input_3d8abc0fac4bec(topic, msg, retained)

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

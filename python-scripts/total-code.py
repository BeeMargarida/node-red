import uasyncio as asyncio
mqtt_client = None
nodes_id = ["376dbd697c4cf2","2408bcc500b93c"]
input_topics = ["topic1_node"]
output_topics = ["topic0_node"]


def on_input(topic, msg, retained):
    topic = topic.decode()
    if topic in input_topics_376dbd697c4cf2:
        on_input_376dbd697c4cf2(topic, msg, retained)
    elif topic in input_topics_2408bcc500b93c:
        on_input_2408bcc500b93c(topic, msg, retained)


async def conn_han(client):
    for input_topic in input_topics:
        await client.subscribe(input_topic, 1)

async def on_output(msg, output):
    for output_topic in output:
        await mqtt_client.publish(output_topic, msg, qos = 1)

def exec(mqtt_c):
    global mqtt_client
    mqtt_client = mqtt_c
    return050d79d8 = ["topic1_node"]
nr_inputs_a82c7a050d79d8 = 1
inputs_a82c7a050d79d8 = []
topics_a82c7a050d79d8 = []

def on_input_a82c7a050d79d8(topic, msg, retained):
    global inputs_a82c7a050d79d8
    glboal topics_a82c7a050d79d8

    if not topic in topics_a82c7a050d79d8:
        topics_a82c7a050d79d8.append(topic)
        if (msg == b'True') or (msg == b'true'):
            inputs_a82c7a050d79d8.append(True)
        elif (msg == b'False') or (msg == b'false'):
            inputs_a82c7a050d79d8.append(False)
    
    if len(topics_a82c7a050d79d8) == nr_inputs_a82c7a050d79d8:
        result = True
        
        for entry in inputs_a82c7a050d79d8:
            result = result and entry
        
        loop = asyncio.get_event_loop()
        loop.create_task(on_output(result, output_topics_a82c7a050d79d8))
        inputs_a82c7a050d79d8 = []
        topics_a82c7a050d79d8 = []
    
    return


def on_input(topic, msg, retained):
    topic = topic.decode()
    if topic in input_topics_915ca7fd3ce4c8:
        on_input_915ca7fd3ce4c8(topic, msg, retained)
    elif topic in input_topics_a82c7a050d79d8:
        on_input_a82c7a050d79d8(topic, msg, retained)


async def conn_han(client):
    for input_topic in input_topics:
        await client.subscribe(input_topic, 1)

async def on_output(msg, output):
    for output_topic in output:
        await mqtt_client.publish(output_topic, msg, qos = 1)

def exec(mqtt_c):
    global mqtt_client
    mqtt_client = mqtt_c
    return
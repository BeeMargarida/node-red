import uasyncio as asyncio
mqtt_client = None
nodes_id = "5b90ba2ab16744", "ab5680609d0f"
input_topics = ["topic0_node"]
output_topics = ["topic0_node", "topic1_node"]

input_topics_5b90ba2ab16744 = ["temperature"]
output_topics_5b90ba2ab16744 = ["topic0_node"]


def on_input_5b90ba2ab16744(topic, msg, retained):
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(msg, output_topics_5b90ba2ab16744))


input_topics_ab5680609d0f = ["topic0_node"]
output_topics_ab5680609d0f = ["topic1_node"]


def if_rule_ab5680609d0f_0(a, b=):
    n = str(a)
    return a == b


def if_function_ab5680609d0f(a):
    res = if_rule_ab5680609d0f_0(a)
    return '%s' % res


def on_input_ab5680609d0f(topic, msg, retained):
    res = if_function_ab5680609d0f(msg)

    # Create task to publish to output topics
    loop = asyncio.get_event_loop()
    loop.create_task(on_output(res, output_topics_ab5680609d0f))


def on_input(topic, msg, retained):
    if input_topics_5b90ba2ab16744 and topic in input_topics_5b90ba2ab16744:
        on_input_5b90ba2ab16744(topic, msg, retained)
    elif input_topics_ab5680609d0f and topic in input_topics_ab5680609d0f:
        on_input_ab5680609d0f(topic, msg, retained)


async def conn_han(client):
    for input_topic in input_topics:
        await client.subscribe(input_topic, 1)


async def on_output(msg, output):
    for output_topic in output:
        await mqtt_client.publish(output_topic, msg, qos=1)


def exec(mqtt_c):
    global mqtt_client
    mqtt_client = mqtt_c
    return
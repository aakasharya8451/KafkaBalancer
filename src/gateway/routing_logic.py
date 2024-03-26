import json


def route(message, topic):
    print(message, topic)
    message_decoded = json.loads(message)

    if message_decoded["message_destination"] == "topic_x":
        if message_decoded["priority"] == 0:
            return (message_decoded["message_destination"], message, message_decoded["priority"])
        elif message_decoded["priority"] == 1:
            return (message_decoded["message_destination"], message, message_decoded["priority"])
    elif message_decoded["message_destination"] == "topic_y":
        if message_decoded["priority"] == 0:
            return (message_decoded["message_destination"], message, message_decoded["priority"])
        elif message_decoded["priority"] == 1:
            return (message_decoded["message_destination"], message, message_decoded["priority"])

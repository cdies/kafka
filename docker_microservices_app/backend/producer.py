# producer send messages to kafka

from time import sleep
import datetime
from confluent_kafka import Producer
import json
from pyorbital.orbital import Orbital
import os


satellite = Orbital('TERRA')

topic = 'test_topic'

bootstrap_servers = os.environ['BOOTSTRAP_SERVERS'] 

producer = Producer({'bootstrap.servers': bootstrap_servers})

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: {}".format(err))
    else:
        print("Produced record to topic {} partition [{}] @ offset {}"
              .format(msg.topic(), msg.partition(), msg.offset()))

# send data every one second
while True:
    time = datetime.datetime.now()
    lon, lat, alt = satellite.get_lonlatalt(time)
    record_value = json.dumps({'lon':lon, 'lat': lat, 'alt': alt, 'time': str(time)})

    producer.produce(topic, key=None, value=record_value, on_delivery=acked)

    producer.poll()
    sleep(1)


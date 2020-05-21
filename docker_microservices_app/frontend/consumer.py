# consumer recive messages from kafka

import datetime
from confluent_kafka import Consumer, TopicPartition
import json
from collections import deque
import os
from time import sleep


class MyKafkaConnect:
    def __init__(self, topic, group, que_len=180):
        self.topic = topic
        bootstrap_servers = os.environ['BOOTSTRAP_SERVERS']

        self.conf = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group,
            'enable.auto.commit': True,
        }

        # the application needs a maximum of 180 data units
        self.data = {
            'time': deque(maxlen=que_len),
            'Latitude': deque(maxlen=que_len),
            'Longitude': deque(maxlen=que_len),
            'Altitude': deque(maxlen=que_len)
        }

        consumer = Consumer(self.conf)
        consumer.subscribe([self.topic])

        # download first 180 messges
        self.partition = TopicPartition(topic=self.topic, partition=0)
        low_offset, high_offset = consumer.get_watermark_offsets(self.partition, timeout=2)

        # move offset back on 180 messages
        if high_offset > que_len:
            self.partition.offset = high_offset - que_len
        else:
            self.partition.offset = low_offset

        # set the moved offset to consumer
        consumer.assign([self.partition])

        self.__update_que(consumer)


    # https://docs.confluent.io/current/clients/python.html#delivery-guarantees
    def __update_que(self, consumer):
        try:
            while True:
                msg = consumer.poll(timeout=0.1)
                if msg is None:
                    break
                elif msg.error():
                    print('error: {}'.format(msg.error()))
                    break
                else:
                    record_value = msg.value()
                    json_data = json.loads(record_value.decode('utf-8'))

                    self.data['Longitude'].append(json_data['lon'])
                    self.data['Latitude'].append(json_data['lat'])
                    self.data['Altitude'].append(json_data['alt'])
                    self.data['time'].append(datetime.datetime.strptime(json_data['time'], '%Y-%m-%d %H:%M:%S.%f'))

                    # save local offset
                    self.partition.offset += 1          
        finally:
            # Close down consumer to commit final offsets.
            # It may take some time, that why I save offset locally
            consumer.close()


    def get_graph_data(self):
        consumer = Consumer(self.conf)
        consumer.subscribe([self.topic])  

        # update low and high offsets (don't work without it)
        consumer.get_watermark_offsets(self.partition, timeout=2)

        # set local offset
        consumer.assign([self.partition])

        self.__update_que(consumer)

        # convert data to compatible format
        o = {key: list(value) for key, value in self.data.items()}
        return o        
        

    def get_last(self):
        lon = self.data['Longitude'][-1]
        lat = self.data['Latitude'][-1]
        alt = self.data['Altitude'][-1]

        return lon, lat, alt


# for test
if __name__ == '__main__':
    connect = MyKafkaConnect(topic='test_topic', group='test_group')

    while True:        
        test = connect.get_graph_data()

        print('number of messages:', len(test['time']), 
            'unique:', len(set(test['time'])), 
            'time:', test['time'][-1].second)

        sleep(0.2)

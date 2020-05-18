# consumer recive messages from kafka

import datetime
from confluent_kafka import Consumer, TopicPartition
import json
from collections import deque
from time import sleep


class MyKafkaConnect:
    def __init__(self, topic, group, que_len=180):
        self.topic = topic

        self.conf = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': group,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
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

        # move offset back on 180 messages
        partition = self.__get_last_offset(consumer)
        if partition.offset > que_len:
            partition.offset = partition.offset - que_len
        else:
            partition.offset = 0

        # set the moved offset to consumer
        consumer.assign([partition])

        self.__update_que(consumer)


    def __get_last_offset(self, consumer):
        partition = TopicPartition(topic=self.topic, partition=0)

        # https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Consumer.get_watermark_offsets
        # get_watermark_offsets returns high offset, (high offset) - 1 = (last message offset)
        last_offset = consumer.get_watermark_offsets(partition)[1] - 1
        partition.offset = last_offset

        return partition


    # https://docs.confluent.io/current/clients/python.html#asynchronous-commits
    def __update_que(self, consumer):
        try:
            while True:
                msg = consumer.poll(timeout=0.1)
                if msg is None:
                    # all messages downloaded
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
            consumer.commit()
        finally:
            consumer.close()


    def get_graph_data(self):
        consumer = Consumer(self.conf)
        consumer.subscribe([self.topic])  

        # set last offset to consumer
        partition = self.__get_last_offset(consumer)
        consumer.assign([partition]) 

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

        print('messages count:', len(test['time']), test['time'][-1])

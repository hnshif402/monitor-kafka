#!/bin/env python

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.errors import kafka_errors

"""
" this program is to seek the position to the end of partition and get the position,
* so we can run this program every 10 mins to get the values which is calculated by current value - last value
* we know how many msg we received during 10 mins,
* if the value is smaller than threshold value, it triger an alarm.
"""

topic_name = "test"
def consume_thread():
    consumer = KafkaConsumer(bootstrap_servers='namenode01:9092', group_id='monitor1')
    tp0 = TopicPartition(topic=topic_name, partition=0)
    tp1 = TopicPartition(topic=topic_name, partition=1)
    consumer.assign([
        tp0,
        tp1
    ])
    """
    print(tp1)
    offsets=consumer.position(tp1)
    print(offsets)
    consumer.seek_to_beginning(tp1)
    offsets=consumer.position(tp1)
    print(offsets)
    """
    consumer.seek_to_end(tp1)
    offsets=consumer.position(tp1)
    print(offsets)
    consumer.close()

if __name__ == "__main__":
    consume_thread()
    

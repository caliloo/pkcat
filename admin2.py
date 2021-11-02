#!/usr/bin/env python3
#
# Copyright 2018 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Example Admin clients.
#

from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions, ConfigResource, ConfigSource, ClusterMetadata
from confluent_kafka import KafkaException, Consumer, TopicPartition
import confluent_kafka as kafka
import sys
import threading
import logging
import time
import datetime

logging.basicConfig()

class Chrono():
    def __init__(self):
        pass

    def start(self):
        self.prev_click = time.time()

    def click(self):
        now = time.time()
        tmp = now - self.prev_click
        self.prev_click = now
        return tmp



def describe(topic):

    adminc = AdminClient({
        'bootstrap.servers': 'localhost'
        })


    c = Consumer({
        'bootstrap.servers': 'localhost',
        'group.id': "mygroup-%u" % int(time.time()),
        'auto.offset.reset': 'earliest'
    })

    

    meta = c.list_topics()


    import code
    code.InteractiveConsole(locals=globals()).interact()




    sys.exit(0)


    event_ready = threading.Event()
    p_list = []

    def on_assign (c, ps):
        for p in ps:
            p.offset=-2

        # c.assign(ps)
        # print('on_assign', ps, flush=True)
        event_ready.set()



    print("subscribing", flush=True)
    c.subscribe([topic], on_assign=on_assign)

    
#offsets_for_times



    while True:

        msg = c.poll(0.1)
        if event_ready.is_set() :
            break

    offsets = {}
    for p in c.assignment() :
        #print(c.get_watermark_offsets(p))
        offsets[p] = c.get_watermark_offsets(p)


    counter = 0
    while True:
        msg = c.poll(0.1)

        if msg is None:
            continue        






    # print(c.position(c.assignment()))
    c.close()    

    return offsets


if __name__ == '__main__':

    broker = 'localhost:9092'

    adminc = AdminClient({
        'bootstrap.servers': 'localhost'
        })


    c = Consumer({
        'bootstrap.servers': 'localhost',
        'group.id': "mygroup-%u" % int(time.time()),
        'auto.offset.reset': 'earliest'
    })

    

    meta = c.list_topics()

    topic = sys.argv[1]

    jobs = []

    for p in meta.topics[topic].partitions.values() :
        offsets = c.get_watermark_offsets(TopicPartition(topic, p.id))
        print()
        jobs.append((topic, p.id, offsets[0], offsets[1]))


    from collections import namedtuple
    PartitionInfo = namedtuple('PartitionInfo',['topic','pid','start_offset', 'end_offset', 'start_time', 'end_time'])

    full_infos = []
    for job in jobs :
        #overrides last assign, so no need for unassign
        c.assign([TopicPartition(job[0],job[1],job[2])])

        if job[3] - job[2] <= 0 :
            print("ignoring empty partition" ,topic , job)
            continue

        got_one = False
        while not got_one:
            msg = c.poll(0.1)
            if msg is None :
                continue

            ts = msg.timestamp()
            if ts[0] == kafka.TIMESTAMP_NOT_AVAILABLE :
                raise Exception("no timestamp available %s" % job)


            start_ts = ts[1]
            got_one = True

        c.seek(TopicPartition(job[0],job[1],job[3]-1))

        got_one = False
        while not got_one:
            msg = c.poll(0.1)
            if msg is None :
                continue

            ts = msg.timestamp()
            if ts[0] == kafka.TIMESTAMP_NOT_AVAILABLE :
                raise Exception("no timestamp available %s" % job)


            end_ts = ts[1]
            got_one = True

        
        full_infos.append(PartitionInfo(job[0], job[1], job[2], job[3], start_ts , end_ts ))


    final_jobs = []
    for i in full_infos :
        print(i)            
        final_jobs.append(PartitionInfo(i.topic, i.pid, i.start_offset, i.end_offset, None, None))





    #assign
    final_assign = []
    for i in final_jobs :
        print(i)    
        final_assign.append(TopicPartition(topic, i.pid, i.start_offset))

    start = time.time()
    c.assign(final_assign)
    print(time.time() - start)
    


    # finished_flag = dict([ (i.pid, False) for i in final_jobs ])
    # stop_at_zero = len(final_jobs)

    reading = dict([ (i.pid, i) for i in final_jobs ])


    counter = 0
    dropped = 0
    crono = Chrono()
    crono.start()
    while len(reading) != 0 :

        msg = c.poll(1.0)
        if msg is None :
            continue

        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            break


        partid = msg.partition()
        offset = msg.offset()

        # if partid not in reading :
        #     print("droppping for", partid, offset)


        # elif (reading[partid].end_offset == offset ):
        #     print("done with %s:%u"% (topic, partid))
        #     del reading[partid]

        #     new_assigns = []
        #     for i in reading.values() :
        #         print(i)    
        #         new_assigns.append(TopicPartition(topic, i.pid, i.start_offset))    
        #     c.assign(final_assign)    


        if partid not in reading :
            dropped += 1
            continue
        elif (reading[partid].end_offset-1 <= offset ):
            #we are with the last message we want
            del reading[partid]


            



        counter += 1
        if counter % 100000 == 0:
            print(100000 / crono.click())

        #print(partid, offset, msg.key(), msg.value())


    print("counter", counter)
    print("dropped", dropped)






    #print(describe(sys.argv[1]))


__author__ = 'sdu'

import pika
import json
import hashlib
import threading
import time
import copy
import random

def between(p_k, p_m, p_n):
    r = 6
    if p_k is None or p_m is None or p_n is None:
        return False
    if p_k == 'None' or p_m == 'None' or p_n == 'None':
        return False
    #k = ring_hash(str(p_k))
    #m = ring_hash(str(p_m))
    #n = ring_hash(str(p_n))
    k = int(str(p_k))
    m = int(str(p_m))
    n = int(str(p_n))
    if m == n:
        return True
    return (m==n or (k<m or k>m)) and (k-m)%2**r <= (n-m)%2**r

def ring_hash(s):
    m = 6
    digest = hashlib.sha1(s).hexdigest()
    return int(digest, 16) % pow(2,m)

parameters = pika.ConnectionParameters(
        'localhost',
        5672,
        '/',
        pika.PlainCredentials('guest','guest'))

class ChordNode(object):
    m = 6
    def __init__(self, node_id, known_id):

        self.pred = None
        self.node_id = str(node_id)
        self.succ = None

        self.M = 2**self.m
        self.fingers = [None for i in range(self.m)]

        # setup channel
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue='findsucc' + str(node_id))
        channel.queue_declare(queue='heresucc' + str(node_id))
        channel.queue_declare(queue='findpred' + str(node_id))
        channel.queue_declare(queue='herepred' + str(node_id))
        channel.queue_declare(queue='notify' + str(node_id))
        channel.basic_consume(self.heresucc_callback,
                              queue='heresucc' + str(node_id),
                              no_ack=True)
        channel.basic_consume(self.findsucc_callback,
                              queue='findsucc' + str(node_id),
                              no_ack=True)
        channel.basic_consume(self.herepred_callback,
                              queue='herepred' + str(node_id),
                              no_ack=True)
        channel.basic_consume(self.findpred_callback,
                              queue='findpred' + str(node_id),
                              no_ack=True)
        channel.basic_consume(self.notify_callback,
                              queue='notify' + str(node_id),
                              no_ack=True)

        if str(node_id) == str(known_id):
            self.succ = str(node_id)
            self.pred = str(node_id)
            print "Listening"
        else:
            print "Searching"
        data = {'curr': str(self.node_id), 'i': str(0)}
        channel.basic_publish(exchange='',
                              routing_key='findsucc' + str(known_id),
                              body=json.dumps(data))
        channel.start_consuming()

    def closest_preceding_finger(self, id):
        for i in range(self.m-1,-1,-1):
            if between(self.finger[i],self.node_id,id):
                return self.finger[i]
        return self.node_id


    def findsucc_callback(self, channel, _, _2, body):
        print "body /" + str(body)
        print "this node /" + str(self.node_id)
        print "my succ node /" + str(self.succ)
        data = json.loads(body)
        pred = data['curr']
        key = (int(pred)+2**(int(data['i'])))%2**self.m
        if between(key, self.node_id, self.succ):
            print "smth"
            data = {'next': self.succ, 'curr': self.node_id, 'i':data['i']}
            channel.basic_publish(exchange='',
                                  routing_key='heresucc'+str(pred),
                                  body=json.dumps(data))
            print "between"
        else:
            channel.basic_publish(exchange='',
                                  routing_key='findsucc'+str(self.closest_preceding_finger(self.succ)),
                                  body=str(body))
            print "not between"


    def heresucc_callback(self, channel, _, _2, body):
        print 'heresucc body/' + str(body)
        data = json.loads(body)
        self.succ = data['next']
        pred = data['curr']
        index = int(data['i'])
        self.fingers[index] = data['next']
        if index==0:
            threading.Thread(target=self.start_stabilise).start()

        if pred != 'none':
            data = {'next': self.node_id, 'curr': 'none', 'i':data['i']}
            channel.basic_publish(exchange='',
                                  routing_key='heresucc'+str(pred),
                                  body=json.dumps(data))

        print "heresucc body/" + str(body)
        print "heresucc this node/" + str(self.node_id)
        print "heresucc my succ node/" + str(self.succ)


    def findpred_callback(self, channel, _, _2, body):
        #print "find body /" + str(body)
        #print "find this node /" + str(self.node_id)
        #print "find my pred node /" + str(self.pred)
        data = {'pred': 'None', 'x': str(self.pred)}
        if between(self.pred, body, self.node_id):
            data['pred'] = 'None'
        channel.basic_publish(exchange='',
                                  routing_key='herepred'+str(body),
                                  body=json.dumps(data))


    def notify_callback(self, channel, _, _2, body):
        if between(body, self.pred, self.node_id) or \
            (self.pred is None) or (self.pred == 'None'):
                self.pred = body
        #print "notify self/" + str(self.node_id)
        #print "notify slef.pred/" + str(self.pred)
        #print "notify body/" + str(body)

    def herepred_callback(self, channel, _, _2, body):
        #print "--I am '"+ str(self.node_id) +"' ."
        #print "--Updated successor to '"+ str(self.succ) +"' ."
        #print "--Updated predecessor to '"+ str(self.pred) +"' ."
        data = json.loads(body)
        #print data
        if data['pred'] != 'None':
            self.succ = data['pred']
        x = data['x']
        channel.basic_publish(exchange='',
                                routing_key='notify'+str(self.succ),
                                body=str(self.node_id))
        print "I am '"+ str(self.node_id) +"' ."
        print "Updated successor to '"+ str(self.succ) +"' ."
        print "Updated predecessor to '"+ str(self.pred) +"' ."

    '''
    def fix_fingers(self, allofthem=False):
        # MyTrace(0,  "fixing fingers")
        if allofthem:
            for i in range(0,self.m):
                self.finger[i] = self.find_successor((self.id + 2**i)%self.M)
                if self.fingers[i] is not None and self.fingers[i].id == self.id:
                    self.fingers[i] = None
        else:
            self.nextval = self.nextval + 1
            if (self.nextval >= self.m):
                self.nextval = 0
            self.fingerss[self.nextval] = self.find_successor((self.id + 2**self.nextval)%self.M)
            if self.finger[self.nextval] is not None and self.fingers[self.nextval].id == self.id:
                    self.fingers[self.nextval] = None

        print 'fingers fixed'
        for i in range(0,self.m):
            if self.finger[i] is not None:
                print 'finger', i, self.finger[i].id
            else:
                print 'finger', i, self.finger[i]

    def notify(self,n):
        if between(n,self.pred,self.node_id) or (self.pred is None):
            self.pred = n

    def stabilise(self):
        x = self.succ.pred
        if between(x,self.node_id,self.succ):
            self.succ = x
        self.succ.notify(self)
    '''

    def fix_fingers(self, channel):
        #for i in range(0,self.m):
        #    key = (int(self.node_id) + 2**i)%self.M
        #    data = {'curr': str(self.node_id), 'i': str(i)}
        #    channel.basic_publish(exchange='',
        #                      routing_key='findsucc' + str(key),
        #                      body=json.dumps(data))
        print self.fingers
        print '-------------FIX--FINGERS-------------------'
        data = {'curr': str(self.node_id), 'i': str(random.randint(0,self.m-1))}
        channel.basic_publish(exchange='',
                              routing_key='findsucc' + str(self.succ),
                              body=json.dumps(data))

    def loop_stabilise(self,channel):
        while True:
            #print "loop stabilise self /" + str(self.node_id)
            #print "self succ /"+ str(self.succ)
            channel.basic_publish(exchange='',
                                  routing_key='findpred'+str(self.succ),
                                  body=str(self.node_id))
            time.sleep(15)
            self.fix_fingers(channel)
            time.sleep(5)

    def start_stabilise(self):
        # create new connection and channel
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        self.loop_stabilise(channel)








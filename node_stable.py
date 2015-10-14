__author__ = 'sdu'

import pika
import json
import hashlib
import threading
import time
import copy

def between(p_k, p_m, p_n):
    if p_k is None or p_m is None or p_n is None:
        return False
    if p_k == 'None' or p_m == 'None' or p_n == 'None':
        return False
    # k = int(str(p_k))
    # m = int(str(p_m))
    # n = int(str(p_n))
    k = ring_hash(str(p_k))
    m = ring_hash(str(p_m))
    n = ring_hash(str(p_n))
    if m == n:
        return True
    r = 64
    return (m==n or (k<m or k>m)) and (k-m)%2**r <= (n-m)%2**r

def ring_hash(s):
    m = 64
    digest = hashlib.sha1(s).hexdigest()
    return int(digest, 16) % pow(2,m)

parameters = pika.ConnectionParameters(
        'localhost',
        5672,
        '/',
        pika.PlainCredentials('guest','guest'))

class ChordNode(object):
    def __init__(self, node_id, known_id):

        self.pred = None
        self.node_id = str(node_id)
        self.succ = None

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
            '''print "Listening"
        else:'''
        print "Searching"
        channel.basic_publish(exchange='',
                              routing_key='findsucc' + str(known_id),
                              body=str(self.node_id))

        channel.start_consuming()


    def findsucc_callback(self, channel, _, _2, body):
        #print "body /" + str(body)
        #print "this node /" + str(self.node_id)
        #print "my succ node /" + str(self.succ)
        if between(body, self.node_id, self.succ):
            #print "smth"
            data = {'next': self.succ, 'curr': self.node_id}
            channel.basic_publish(exchange='',
                                  routing_key='heresucc'+str(body),
                                  body=json.dumps(data))
            #print "between"
        else:
            channel.basic_publish(exchange='',
                                  routing_key='findsucc'+str(self.succ),
                                  body=str(body))
            #print "not between"


    def heresucc_callback(self, channel, _, _2, body):
        data = json.loads(body)
        self.succ = data['next']
        curr = data['curr']

        if curr != 'none':
            data = {'next': self.node_id, 'curr': 'none'}
            channel.basic_publish(exchange='',
                                  routing_key='heresucc'+str(curr),
                                  body=json.dumps(data))
            threading.Thread(target=self.start_stabilise).start()
        #print "body /" + str(body)
        #print "this node /" + str(self.node_id)
        #print "my succ node /" + str(self.succ)


    def findpred_callback(self, channel, _, _2, body):
        #print "find body /" + str(body)
        #print "find this node /" + str(self.node_id)
        #print "find my pred node /" + str(self.pred)
        data = {'pred': 'None', 'x': str(self.pred)}
        if between(self.pred, body, self.node_id):
            data = {'pred': str(self.pred), 'x': str(self.pred)}
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

    '''def notify(self,n):
        if between(n,self.pred,self.node_id) or (self.pred is None):
            self.pred = n

    def stabilise(self):
        x = self.succ.pred
        if between(x,self.node_id,self.succ):
            self.succ = x
        self.succ.notify(self)
    '''

    def loop_stabilise(self,channel):
        while True:
            #print "loop stabilise self /" + str(self.node_id)
            #print "self succ /"+ str(self.succ)
            channel.basic_publish(exchange='',
                                  routing_key='findpred'+str(self.succ),
                                  body=str(self.node_id))
            time.sleep(20)

    def start_stabilise(self):
        # create new connection and channel
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        self.loop_stabilise(channel)







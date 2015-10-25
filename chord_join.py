__author__ = 'sdu'

import pika
import json
import hashlib

def between(p_k, p_m, p_n):
    if p_k is None or p_m is None or p_n is None:
        return False
    if p_k == 'None' or p_m == 'None' or p_n == 'None':
        return False
    # k = int(p_k)
    # m = int(p_m)
    # n = int(p_n)
    k = ring_hash(p_k)
    m = ring_hash(p_m)
    n = ring_hash(p_n)
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
        # setup channel
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue='findsucc' + str(node_id))
        channel.queue_declare(queue='heresucc' + str(node_id))
        channel.basic_consume(self.heresucc_callback,
                              queue='heresucc' + str(node_id),
                              no_ack=True)
        channel.basic_consume(self.findsucc_callback,
                              queue='findsucc' + str(node_id),
                              no_ack=True)

        self.node_id = node_id
        self.succ = None

        if node_id == known_id:
            self.succ = node_id
            print "Listening"
        else:
            print "Searching"
            channel.basic_publish(exchange='',
                                  routing_key='findsucc' + str(known_id),
                                  body=self.node_id)

        channel.start_consuming()


    def findsucc_callback(self, channel, _, _2, body):
        print "body /" + str(body)
        print "from node /" + str(self.node_id)
        print "my succ /" + str(self.succ)
        if between(body, self.node_id, self.succ):
            data = {'next': self.succ, 'prev': self.node_id}
            channel.basic_publish(exchange='',
                                  routing_key='heresucc'+body,
                                  body=json.dumps(data))
        else:
            channel.basic_publish(exchange='',
                                  routing_key='findsucc'+self.succ,
                                  body=body)


    def heresucc_callback(self, channel, _, _2, body):
        data = json.loads(body)
        self.succ = data['next']
        prev = data['prev']

        if prev != 'None':
            data = {'next': self.node_id, 'prev': 'None'}
            channel.basic_publish(exchange='',
                                  routing_key='heresucc'+prev,
                                  body=json.dumps(data))







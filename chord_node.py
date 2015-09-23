__author__ = 'sdu'

import pika
import hashlib

class ChordNode(object):

    def __init__(self, node_id,known_id):
        "set up channel"
        channel.queue_declare('findsucc'+node_id)
        channel.queue_declare('heresucc'+node_id)
        channel.basic_consume()
        channel.n            ()
        + channel.start_consuming()
        self.node_id = node_id
        self.succ = None
        if node_id = known_id:
            self.succ = node_id
        else:
            channel.basic_publish(exchange='',
                                  routing_key='findsucc'+known_id,
                                  body=node_id)


def findsucc_callback(self,ch,_,_,body):
    if between(body, self.node_id,self.succ):
        ch.basic_publish(exchange,
                         routing_key='heresucc'+body,
                         body=self.succ)
    else:
        ch.basic_publish(exchange='',routing_key='findsucc'+self.succ,body=body)

m = 64
def ring_hash(s):
    digest = hashlib.sha1(s).hexdigest()
    return int(digest, 16) % pow(2,m)

def heresucc_callback(self,ch,_,_,body):
    self.succ = bodyd

''' // ask node n to find the successor of id
 n.find_successor(id)
   //Yes, that should be a closing square bracket to match the opening parenthesis.
   //It is a half closed interval.
   if (id \in (n, successor] )
     return successor;
   else
     // forward the query around the circle
     n0 = closest_preceding_node(id);
     return n0.find_successor(id);


// search the local table for the highest predecessor of id
 n.closest_preceding_node(id)
   for i = m downto 1
     if (finger[i]\in(n,id))
       return finger[i];
   return n;
The pseudocode to stabilize the chord ring/circle after node joins and departures is as follows:

 // create a new Chord ring.
 n.create()
   predecessor = nil;
   successor = n;
 // join a Chord ring containing node n'.
 n.join(n')
   predecessor = nil;
   successor = n'.find_successor(n);
 // called periodically. n asks the successor
 // about its predecessor, verifies if n's immediate
 // successor is consistent, and tells the successor about n
 n.stabilize()
   x = successor.predecessor;
   if (x\in(n, successor))
     successor = x;
   successor.notify(n);
 // n' thinks it might be our predecessor.
 n.notify(n')
   if (predecessor is nil or n'\in(predecessor, n))
     predecessor = n';
 // called periodically. refreshes finger table entries.
 // next stores the index of the finger to fix
 n.fix_fingers()
   next = next + 1;
   if (next > m)
     next = 1;
   finger[next] = find_successor(n+2^{next-1});
 // called periodically. checks whether predecessor has failed.
 n.check_predecessor()
   if (predecessor has failed)
     predecessor = nil;
'''










#!/usr/bin/env python                                                                                                                                                          

import sys
import os
import pika
import logging

from retrying import retry

from config import Configuration

me = os.path.basename (sys.argv[0])

def usage ():
    print "Usage: " + me + "<some string>"
    sys.exit (1)

if len (sys.argv) != 2:
    usage ()

# Make stupid pika errors go away
logging.basicConfig (format='%(levelname)s:%(message)s', level=logging.CRITICAL)

message = sys.argv[1]
config = Configuration ()

@retry(wait_exponential_multiplier=100, wait_exponential_max=300000)
def get_connection():

    try:
        user_credentials = pika.PlainCredentials (config.user_name, config.user_pw)
        mq_connection = pika.BlockingConnection (pika.ConnectionParameters (host = config.host_name, credentials = user_credentials))
        mq_channel = mq_connection.channel ()

    except pika.exceptions.AMQPConnectionError as error:
        print "  MQ Connection Error, retrying..."

    return mq_channel, mq_connection

def mq_push (message):
    mq_channel, mq_connection = get_connection ()
    print " [x] MQ Connection Acquired"
    mq_channel.basic_publish (exchange = '', routing_key = config.queue_name, body = message,
                              properties = pika.BasicProperties (delivery_mode = 2)) # make message persistent
    print " [x] Sent %r" % (message,)
    mq_connection.close ()


mq_push (message)
sys.exit (0)

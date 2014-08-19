#!/usr/bin/env python                                                                                                                                                          

# Test cases:

# Starts eventually when mq is initially down
# Shuts down with interrrupt when idle
# Shuts down with interrrupt after finishing current job
# When rabbit goes away while idle, reconnects and processes properly, then processes jobs
# When rabbit goes away while procesing, finishes current job, then connects and does not re-process job just processed
# Handles exceptions from job by removing message from queue

import sys
import signal
import os
import pika
import logging
import time

from retrying import retry

from config import Configuration


me = os.path.basename (sys.argv[0])
config = Configuration ()

def usage ():
    print "Usage: " + me
    sys.exit (1)

if len (sys.argv) != 1:
    usage ()


def write (message):
    sys.stdout.write (message)
    sys.stdout.flush ()
    

# Make stupid pika errors go away
logging.basicConfig (format='%(levelname)s:%(message)s', level=logging.CRITICAL)


class ShutdownRequested (Exception):
    SIGNAL_NAMES = dict ((getattr (signal, s), s) for s in dir(signal) if s.startswith('SIG') and '_' not in s)

    def __init__ (self, value):
        self.value = value
        self.name = self.SIGNAL_NAMES[value]

    def __str__ (self):
        return repr (self.value)

currently_processing = False
shutdown_requested = False

def signal_handler (_signo, _stack_frame):
    global currently_processing
    global shutdown_requested

    print "signal_handler called: " + str (_signo)
    shutdown_requested = _signo
    if not currently_processing:
        raise ShutdownRequested (_signo)

def work_function (body):
    global currently_processing

    print " [x] Received %r" % (body,)
    write ("Working .")
    if body == "Throw":
        raise ValueError
    for i in range(3):
        time.sleep (2)
        write (".")

    print ("Done with %r" % (body,))

def callback (ch, method, properties, body):
    global currently_processing
    global shutdown_requested

    last_tag = method.delivery_tag

    try:
        currently_processing = True
        work_function (body)

    # This exception is never caught here - it's caught in consume_queue
    except pika.exceptions.AMQPConnectionError as error:
        print "callback: AMQPConnectionError."

    # Here if work_function throws an exception - remove the evil message
    except ValueError as e:
        print "callback: Exception: ValueError" + str (e)

    finally:
        ch.basic_ack (delivery_tag = method.delivery_tag)
        currently_processing = False
        if shutdown_requested:
            raise ShutdownRequested (shutdown_requested)

def consume_queue(channel, connection):

    print ' [*] Waiting for messages. To exit press CTRL+C'

    try:
        channel.start_consuming()
        print "consume_queue: start_consuming returned"

    except ShutdownRequested as s:
        print "consume_queue: Exception: ShutdownRequested " + s.name
        channel.stop_consuming()
        connection.close()
        return 'SHUTDOWN_REQUESTED'

    except pika.exceptions.AMQPConnectionError as error:
        print " consume_queue: Exception: Lost MQ Connection."
        return 'LOST_MQ_CONNECTION'

    except Exception as e:
        print " consume_queue: Exception: Unknown"
        return 'LOST_MQ_CONNECTION'

@retry(wait_exponential_multiplier=100, wait_exponential_max=300000)
def get_connection (creds):
    try:
        connection = pika.BlockingConnection (pika.ConnectionParameters (host = config.host_name, credentials = creds))

    except pika.exceptions.AMQPConnectionError:
        print "MQ connection attempt failed. Retrying..."
        raise

    except Exception as e:
        print "MQ connection attempt - UNKNOWN. Retrying..."
        channel = None
        connection = None
        raise

    except ShutdownRequested as s:
        print " [*] Shutting down"
        sys.exit (0)

    channel = connection.channel ()
    channel.queue_declare (queue = config.queue_name, durable = True)
    channel.basic_qos (prefetch_count = 1)
    channel.basic_consume (callback, queue = config.queue_name)

    return channel, connection


credentials = pika.PlainCredentials (config.user_name, config.user_pw)
signal.signal (signal.SIGTERM, signal_handler)
signal.signal (signal.SIGINT, signal_handler)    # This handles KeyboardInterrupt, which is SIGINT

while True:

    channel, connection = get_connection (credentials)
    print " [*] Connection acquired"

    ret_val = consume_queue (channel, connection)
    if ret_val == 'SHUTDOWN_REQUESTED':
        print " [*] Shutting down"
        sys.exit (0)
    else:
        print " [*] Lost MQ connection. Attempting to reconnect..."

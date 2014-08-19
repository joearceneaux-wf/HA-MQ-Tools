HA-MQ-Tools
===========

Clients for testing availability of MQ

**ha_pull.py**

Pulls messages from MQ and pretends to work on them. If connection to MQ is lost, it trys to re-establish it.

**ha_push.py**

Pushes messages to MQ. If the connection is missing, continues to attempt connection.

**config.py**

Values such as the MQ server, the queue name, etc.

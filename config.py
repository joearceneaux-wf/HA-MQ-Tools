
class Configuration:

    def __init__ (self):
        self.host_name = "localhost"
        self.queue_name = "ha_tests"
        self.user_name = "user1"
        self.user_pw = "password1"
        self.retry_times = [ 10, 8, 4, 2, 2, 2 ]

class SmithyConfiguration:

    def __init__ (self):
        # self.host_name = "10.34.49.229"
        # self.host_name = "rabbitmq-staging.webfilings.org"
        self.host_name = "ec2-54-225-122-248.compute-1.amazonaws.com"
        self.queue_name = "smithy_tests"
        self.user_name = "smithy"
        self.user_pw = "changeme"
        self.retry_times = [ 4, 2, 2, 2 ]

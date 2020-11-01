# Input
URL = 'https://stream.meetup.com/2/rsvps'

# Kafka Local
BROKER = 'localhost'
PORT = '9092'
TOPIC_NAME = 'quickstart-events'
GROUP_NAME = 'group-local-test'

# Kafka Cluster
# BROKER = 'ip-172-31-91-232.ec2.internal'
# PORT = '9092'
# TOPIC_NAME = 'rsvp_jaye'
# GROUP_NAME = 'group_rsvp_jaye'

# Local Storage
SINK = 'target/rsvp_sink'
CHECK_POINT = 'target/rsvp_checkpoint/checkpoint0'

# Cluster Storage
# SINK = '/user/jiayiyang/rsvp/rsvp_sink'
# CHECK_POINT = '/user/jiayiyang/rsvp/rsvp_checkpoint'

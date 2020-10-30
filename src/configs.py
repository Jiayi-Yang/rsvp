# Input
URL = 'https://stream.meetup.com/2/rsvps'

# kafka local
BROKER = 'localhost'
PORT = '9092'
TOPIC_NAME = 'quickstart-events'
GROUP_NAME = 'group-local-test'

# kafka cluster
# BROKER = 'ip-172-31-91-232.ec2.internal'
# PORT = '9092'
# TOPIC_NAME = 'rsvp-jaye'
# GROUP_NAME = 'group_rsvp_jaye'

# Storage
SINK = '/user/jiayiyang/rsvp/rsvp_sink'
CHECK_POINT = '/user/jiayiyang/rsvp/rsvp_checkpoint'

#!/usr/bin/env bash

# Copy to Jumpbox
scp src/stream/q2.py jiayiyang@54.86.193.122:~
ssh jiayiyang@54.86.193.122

# Copy to Edge Node
scp q2.py jiayiyang@ip-172-31-92-98.ec2.internal:~

# Run
/opt/cloudera/parcels/CDH/bin/spark-submit \
--master yarn \
--deploy-mode client \
q4.py

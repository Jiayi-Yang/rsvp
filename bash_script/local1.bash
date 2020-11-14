#!/usr/bin/env bash

# Run from Local Terminal
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1\
 --master local \
 /Users/jay/Documents/project/pythonProject/rsvp/src/stream/q1.py
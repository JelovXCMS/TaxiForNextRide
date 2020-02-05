import logging
from ksql import KSQLAPI

logging.basicConfig(level=logging.DEBUG)
client = KSQLAPI('ec2-52-41-32-196.us-west-2.compute.amazonaws.com:8088')


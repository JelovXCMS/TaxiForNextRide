import logging
from ksql import KSQLAPI

logging.basicConfig(level=logging.DEBUG)
#client = KSQLAPI('http://ec2-52-41-32-196.us-west-2.compute.amazonaws.com:8088')
client = KSQLAPI('http://10.0.0.13:8088')

#client.create_stream()

client.ksql('show tables')
#client = KSQLAPI('http://ec2-52-41-32-196.us-west-2.compute.amazonaws.com:8088')


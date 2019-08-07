import os
import sys
import time
import logging

import grpc

from grpc_cluster.service.auth import JWTAuthClient


client = JWTAuthClient('localhost:6000')

try:
    res = client.login('123', '456')
except Exception as e:
    print(e)

res = client.login('hello', 'world')

print(res)



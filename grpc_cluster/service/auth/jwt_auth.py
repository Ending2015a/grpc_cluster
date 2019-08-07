import abc

import os
import sys
import time
import logging

import uuid
import hashlib
import datetime
import binascii

import jwt
import grpc

from concurrent import futures

from grpc_cluster.service.rpc.auth import jwt_auth_pb2 as jwt_pb2
from grpc_cluster.service.rpc.auth import jwt_auth_pb2_grpc as jwt_pb2_grpc
from grpc_cluster.service.rpc.auth import jwt_auth_type_pb2 as jwt_type

from grpc_cluster.resource.db import DB, Query

from grpc_cluster.common import utils


# === JWT utils ===

def doHash(raw_password, salt, iterations=100000, hash_name='sha256'):
    dk = hashlib.pbkdf2_hmac(password=raw_password.encode('utf-8'),
                             salt=salt.encode('utf-8'),
                             iterations=iterations,
                             hash_name=hash_name)

    return binascii.hexlify(dk).decode('ascii')


def encode(payload, secret, algorithm='HS256'):
    token = jwt.encode(payload, secret, algorithm=algorithm)
    return token


def decode(encoded, secret, algorithms=['HS256']):
    payload = jwt.decode(encoded, secret, algorithms=algorithms)
    return payload

# =================


# === UserField ===
class UserField:
    id = None
    username = None
    password = None
    salt = None

    @classmethod
    def create(cls, username, raw_password, id=None, salt=None):
        u = cls()
        if id is None:
            id = str(uuid.uuid4())

        if salt is None:
            salt = uuid.uuid4().hex

        u.id = id
        u.salt = salt
        u.username = username
        u.password = doHash(raw_password, salt)

        return u
# =================



class Servicer(jwt_pb2_grpc.AuthenticationServicer):

    def __init__(self, db_path='resource/jwt_auth.db.json'):
        self.db_path = db_path

        # create directories
        dirname = os.path.dirname(db_path)

        if dirname and (not os.path.isdir(dirname)):
            utils.makedirs(dirname)

        self.db = DB(db_path)


    def addUser(self, username, password):

        user = UserField.create(username, password)

    @staticmethod
    def generateToken(user_info):
        payload = {
                'iss': 'Zexlus server',
                'iat': int(time.time()),
                'exp': int(time.time()) + 86400 * 7,
                'aud': 'app.zexlus.nctu.me',
                'sub': user_info['id'],
                'username': user_info['username']
                }

        token = jwt.encode(payload, JWT_SECRET, algorithm='HS256')

        return token

    def varifyUser(self, username, raw_password):
        if username == self.username:
            password = self.hashPassword(raw_password, self.salt)

            return password == self.password
                
        return False

    def Login(self, request, context):
        username = request.username
        password = request.password

        if self.varifyUser(username, password):
            user_info = {
                    'id': self.id,
                    'username': username
                    }
            token = self.generateToken(user_info)

            print('[server side] token={}'.format(token))

            return jwt_type.LoginResponse(token=token)

        context.set_code(grpc.StatusCode.PERMISSION_DENIED)
        context.set_details('Invalid password')

        return jwt_type.LoginResponse()



class Server():
    def __init__(self, username, password, max_workers=3):
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))

        self.servicer = JWTAuthServicer(str(username), str(password))

        jwt_pb2_grpc.add_AuthenticationServicer_to_server(self.servicer, self.server)

    def start(self, port, block=False):
        self.server.add_insecure_port('[::]:{}'.format(port))

        self.server.start()
        print('server listening at port: {}'.format(port))

        try:
            time.sleep(60*60*24)

        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        time.sleep(1)
        self.server.stop(0)

class Client():
    def __init__(self, address):
        channel = grpc.insecure_channel(address)
        self.stub = jwt_pb2_grpc.AuthenticationStub(channel)

    def login(self, username, password):
        request = jwt_type.LoginRequest(username=username, password=password)

        response = self.stub.Login(request)

        return response

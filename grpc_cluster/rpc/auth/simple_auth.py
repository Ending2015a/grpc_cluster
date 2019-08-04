import abc

from grpc_cluster.common import common_type

import logging
from grpc_cluster.logger import *

import datetime

import hashlib
import binascii
import json

class ABCLowEndAuthenticationServicer(abc.ABC):

    @abc.abstractmethod
    def _restoreTokens(self, token_file_path):
        #TODO:
        #   restore tokens from token_file_path
        raise NotImplementedError('Method not implemented!')
    
    @abc.abstractmethod
    def _restoreUsers(self, user_file_path):
        #TODO:
        #   restore User information from user_file_path
        raise NotImplementedError('Method not implemented!')
    
    @abc.abstractmethod
    def _saveTokens(self, token_file_path):
        #TODO:
        #   save tokens to token_file_path
        raise NotImplementedError('Method not implemented!')
    
    @abc.abstractmethod
    def _saveUsers(self, user_file_path):
        #TODO:
        #   save User information to user_file_path
        raise NotImplementedError('Method not implemented!')
    
    @abc.abstractmethod
    def _validateToken(self, token):
        #TODO:
        #   validate if the token is valid
        raise NotImplementedError('Method not implemented!')
    
    @abc.abstractmethod
    def _validateUser(self, username):
        #TODO:
        #   validate if the username already existed in the user info list
        raise NotImplementedError('Method not implemented!')

    def _validatePassword(self, username, password):
        #TODO:
        #   validate if the password is correct
        raise NotImplementedError('Method not implemented!')

    @abc.abstractmethod
    def _updateUserToken(self, username, token):
        #TODO:
        #   update username, token pairs
        raise NotImplementedError('Method not implemented!')


    @abc.abstractmethod
    def _updateUserInfo(self, username, info):
        #TODO:
        # update user information
        raise NotImplementedError('Method not implemented!')

    @abc.abstractmethod
    def _generateToken(self, username, password):
        #TODO:
        #   generate token using username, password and datetime
        raise NotImplementedError('Method not implemented!')
        # create seed
        #seed = (username + password + datetime.datetime.now().strftime('%Y%m%d%H%M%S')).encode('utf-8')
        
        # generate token with seed
        # digest() -> bytes
        #token_value = hashlib.sha256(seed).digest()

        # return Token object
        #return common_type.Token(value=token_value)

    @abc.abstractmethod
    def _generateUser(self, username):
        #TODO:
        # generate new user information
        raise NotImplementedError('Method not implemented!')


class LowEndAuthenticationServicer(ABCLowEndAuthenticationServicer):
    def __init__(self, token_file_path='token.json', 
                        user_file_path='user.json', 
                        clear=False, 
                        logger_name='LowEndAuthenticationServicer', 
                        logger_level='INFO'):
        
        self._initializeLogger(logger_name, logger_level)

        self.token_file_path = token_file_path
        self.user_file_path = user_file_path
       
        if clear:
            self._initializeUsers(user_file_path)
            self._initializeTokens(token_file_path)
        else:
            self._restoreUsers(user_file_path)
            self._restoreTokens(token_file_path)


    def _initializeLogger(self, logger_name, logger_level):
        
        loadConfig()

        self.LOG = createLoggerFromExistedLogger(logger_name, logger_level)



    def _changeTokenPath(self, new_path):
        self.token_file_path = new_path
        self._saveTokens(new_path)

    def _changeUserInfoPath(self, new_path):
        self.user_file_path = new_path
        self._saveUsers(new_path)



    def _serializeToken(self, token):
        # input: binary token
        # output: str token
        return binascii.hexlify(token).decode('ascii')
    
    def _deserializeToken(self, token):
        # input str token
        # output: binary token
        return binascii.unhexlify(token.encode('ascii'))



    def _initializeTokens(self, token_file_path=""):
        self.tokens_token_to_user = {}
        self.tokens_user_to_token = {}

        self._saveTokens()

    def _initializeUsers(self, user_file_path=""):
        self.users = {}
        
        self._saveUsers()



    def _restoreTokens(self, token_file_path=""):
        if token_file_path == "":
            token_file_path = self.token_file_path

        # initalize dictionary
        self.tokens_token_to_user = {}
        self.tokens_user_to_token = {}

        try:
            # read json file
            with open(token_file_path) as json_file:
                user_strtokens = json.load(json_file)

            # deserialize tokens
            for username in user_strtokens:
                strtoken = user_strtokens[username]
                token = self._deserializeToken(strtoken)
                self.tokens_token_to_user[token] = username
                self.tokens_user_to_token[username] = token

        except FileNotFoundError:

            self._saveTokens()

    def _restoreUsers(self, user_file_path=""):
        if user_file_path == "":
            user_file_path = self.user_file_path

        self.users = {}

        try:
            # read json file
            with open(user_file_path) as json_file:
                self.users = json.load(json_file)

        except FileNotFoundError:
            self._saveUsers()



    def _saveTokens(self, token_file_path=""):
        if token_file_path == "":
            token_file_path = self.token_file_path

        user_strtokens = {}
        
        # serialize tokens
        for username in self.tokens_user_to_token:
            token = self.tokens_user_to_token[username]
            strtoken = self._serializeToken(token)
            user_strtokens[username] = strtoken
        
        # dump to json file
        with open(token_file_path, 'w') as json_file:
            json.dump(user_strtokens, json_file)


    def _saveUsers(self, user_file_path=""):
        if user_file_path == "":
            user_file_path = self.user_file_path

        # dump to json file
        with open(user_file_path, 'w') as json_file:
            json.dump(self.users, json_file)



    
    def _validateToken(self, token):
        return token in self.tokens_token_to_user

    def _validateUser(self, username):
        return username in self.users

    def _validatePassword(self, username, password):
        passwd = self._getEncryptedPassword(password)

        return self.users[username]['password'] == passwd



    def _updateUserToken(self, username, new_token, old_token=""):

        if old_token != "":
            del self.tokens_token_to_user[old_token]

        self.tokens_token_to_user[new_token] = username
        self.tokens_user_to_token[username] = new_token
    

    def _updateUserInfo(self, username, info):
        self.users[username] = info



    def _generateToken(self, username, password):

        seed = (username + password + datetime.datetime.now().strftime('%Y%m%d%H%M%S')).encode('utf-8')

        # binary
        token_value = hashlib.sha256(seed).digest()

        self._updateUserToken(username, token_value)
        return token_value

    def _generateUser(self, username, password):

        now = datetime.datetime.now()
        
        user_info = { 'username': username,
                      'password': self._getEncryptedPassword(password),
                      'reg_time': datetime.datetime.timestamp(now),
                      'last_login': datetime.datetime.timestamp(now),
                      'login_count': 1,
                    }

        self._updateUserInfo(username, user_info)

        return user_info



    def _getEncryptedPassword(self, password):
        seed = password.encode('ascii')
        return hashlib.md5(seed).hexdigest()

    def _getUsernameByToken(self, token):
        return self.tokens_token_to_user[token]

    def _getUserInfo(self, username):
        # get user information (not safe)
        return dict(self.users[username])
        
    def _getTokenByUsername(self, username):
        return self.tokens_user_to_token[username]


class ABCAuthenticationServicer(abc.ABC):

    @abc.abstractmethod
    def validateUser(self, username):
        #TODO:
        # 
        raise NotImplementedError('Method not implemented!')

    @abc.abstractmethod
    def getUserInfo(self, username):
        #TODO:
        # return user information (copy) if user exists otherwise return None
        raise NotImplementedError('Method not implemented!')

    @abc.abstractmethod
    def getUsernameByToken(self, token):
        #TODO:
        # 
        raise NotImplementedError('Method not implemented!')

    @abc.abstractmethod
    def getUserInfoByToken(self, token):
        #TODO:
        # 
        raise NotImplementedError('Method not implemented!')

    def validateToken(self, token):
        #TODO:
        #    type(token) == common_type.Token
        #    validate if the token is valid
        #    return True if the token is valid, othewise return False
        
        raise NotImplementedError('Method not implemented!')

    @abc.abstractmethod
    def authenticateToken(self, token):
        #TODO:
        #    type(token) == common_type.Token
        #    validate if the token is valid
        #    if the token is invalid, an Exception will be raised

        raise NotImplementedError('Method not implemented!')

    @abc.abstractmethod
    def registerNewUser(self, username, password):
        #TODO:
        #    check whether the new user is not in the user list:
        #     if it is not in the list than add to the list
        #      and create user token
        #     otherwise return None

        #token = generateToken(username, password)

        #TODO:
        #    store token to the the token list

        #return token
        raise NotImplementedError('Method not implemented!')

    @abc.abstractmethod
    def loginUser(self, username, password):
        #TODO:
        #   validate user, update token
        raise NotImplementedError('Method not implemented!')


class AuthenticationServicer(ABCAuthenticationServicer, LowEndAuthenticationServicer):


    def __init__(self, token_file_path='token.json', 
                        user_file_path='user.json', 
                        clear=False, 
                        logger_name='AuthenticationServicer', logger_level='INFO'):
    
        LowEndAuthenticationServicer.__init__(self=self,
                                              token_file_path=token_file_path,
                                              user_file_path=user_file_path,
                                              clear=clear,
                                              logger_name=logger_name,
                                              logger_level=logger_level)
    
    def validateUser(self, username):
        return self._validateUser(username)

    def getUserInfo(self, username):
        # get user information  (safe)
        if not self._validateUser(username):
            self.LOG.error('user: {} does not exist'.format(username))
            raise Exception('username: {} does not exist'.format(username))
            #TODO: user does not exist error
            pass
        return self._getUserInfo(username)

    def getUsernameByToken(self, token):
        if not self._validateToken(token.value):
            self.LOG.error('invalid token: {}'.format(token.value))
            raise Exception('invalid token')
        
        username = self._getUsernameByToken(token.value)
        
        return username

    def getUserInfoByToken(self, token):
        username = self.getUsernameByToken(token)
        return self._getUserInfo(username)
    
    def validateToken(self, token):
        return self._validateToken(token.value)


    def authenticateToken(self, token):
        # type(token) == common_type.Token
        #
        #    message Token{
        #        bytes value = 1;        
        #    }
        if not self._validateToken(token.value):
            self.LOG.error('invalid token')
            raise Exception('invalid token'.format(token.value))



    def registerNewUser(self, username, password):
        if self._validateUser(username):
            self.LOG.error('user: {} already exists'.format(username))
            raise Exception('user: {} already exists'.format(username))
            #TODO: user already exist error
            pass
        # generate & update
        token = self._generateToken(username, password)

        # generate & update
        user_info = self._generateUser(username, password)
        
        self.LOG.info('user: {} has beed created'.format(username))

        self._saveTokens()
        self._saveUsers()

        # return common_type.Token
        return common_type.Token(value=token)



    def loginUser(self, username, password):
        if not self._validateUser(username):
            self.LOG.error('user: {} does not exist'.format(username))
            raise Exception('username: {} does not exist'.format(username))
            #TODO: user not exist error
            pass

        if not self._validatePassword(username, password):
            self.LOG.error('user: {} wrong password: {}'.format(username, password))
            raise Exception('username: {} wrong password: {}'.format(username, password))
            #TODO: wrong password error    
            pass

        # generate & update
        # self._generateToken(username, password)
        token = self._getTokenByUsername(username)

        user_info = self._getUserInfo(username)

        # update user info
        now = datetime.datetime.now()
        user_info['last_login'] = datetime.datetime.timestamp(now)
        user_info['login_count'] += 1


        self._updateUserInfo(username, user_info)

        self._saveTokens()
        self._saveUsers()
        
        # return common_type.Token
        return common_type.Token(value=token)





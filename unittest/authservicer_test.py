from grpc_cluster.server import AuthenticationServicer

servicer = AuthenticationServicer('token.json', 'user.json', clear=True, logger_level='ERROR')

TEST_CLEAR=True

# test login before resigter
try:
    token = servicer.loginUser('joehsiao', 'pass')

    print('user joehsiao login successful')
    
    TEST_CLEAR=False

except Exception as e:
    print(e)

assert TEST_CLEAR ==True

# test register new user
try:
    token = servicer.registerNewUser('joehsiao', 'pass')

    print('user joehsiao register successful')

except Exception as e:
    print(e)
    TEST_CLEAR=False

assert TEST_CLEAR == True

# test register same user
try:
    token = servicer.registerNewUser('joehsiao', 'hello')

    print('user joehsiao register successfule')
 
    TEST_CLEAR=False

except Exception as e:
    print(e)

assert TEST_CLEAR == True

# test login as registered user with wrong password
try:
    token = servicer.loginUser('joehsiao', 'hello')

    print('user joehsiao login successful')
    
    TEST_CLEAR=False
except Exception as e:
    print(e)


assert TEST_CLEAR == True

# test login as registered user with correct password
try:
    token = servicer.loginUser('joehsiao', 'pass')

    print('user joehsiao login successful')

except Exception as e:
    print(e)
    TEST_CLEAR=False

assert TEST_CLEAR == True

# test validate token
try:
    if servicer.authenticateToken(token):
        print('valid token')
    else:
        print('invalid token')
        TEST_CLEAR=False

except Exception as e:
    print(e)
    TEST_CLEAR=False

assert TEST_CLEAR == True

# test register multiple users
try:
    user_list = ['hello', 'foo', 'bar', 'ddfdsdfds', '大家好']
    pass_list = ['aaa',   'bar', 'foo', 'fdfdsdsee', 'hello']
    

    for user, passwd in zip(user_list, pass_list):

        servicer.registerNewUser(user, passwd)

except Exception as e:
    print(e)
    TEST_CLEAR=False

assert TEST_CLEAR == True

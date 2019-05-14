from grpc_cluster.proxy import DefaultProxyClient
client = DefaultProxyClient('localhost:50051')


def test(client):
    res = client.welcome()
    
    assert res != None
    
    print('get welcome message: ', res)

    assert client.checkUser('admin') == True
    
    admin_token = client.login('admin', 'admin')
    
    assert admin_token != None
    
    if not client.checkUser('joehsiao'):
        print('register new user')
        client._register('joehsiao', 'joe')

        assert client.checkUser('joehsiao') == True

    assert client.login('joehsiao', 'joe')
    
    assert client.sendMessage('hello! I\'m Joe.')
    
    assert client.upload('img/001.jpg', 'test/uploaded_001.jpg')

    assert client.download('img/002.jpg', 'test/downloaded_002.jpg')

    assert client.execute('mkdir joe_directory')

    assert client.createVenv('venv',[
                                    'pyyaml',
                                    'gym'])
    
    

if __name__ == '__main__': 
    test(client)

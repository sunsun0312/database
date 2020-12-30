import os
import sys
import requests

zid = ''
# set the base url
BASE_URL = 'http://127.0.0.1:5000/'

data = []

try:
    response = requests.get(BASE_URL)
except:
    print("请检查flask程序是否开启")
    exit()


def test_empty():
    """
    测试在没有post之前的error
    """
    # test collections/{id}
    U1 = BASE_URL + 'collections/1'
    response = requests.get(U1)
    assert response.status_code != 500

    # test collections
    response = requests.get(BASE_URL)
    assert response.status_code != 500

    # test delete
    response = requests.delete(U1)
    assert response.status_code != 500

    # test collections/{id}/{year}
    U1 = BASE_URL + 'collections/1/2012'
    response = requests.get(U1)
    assert response.status_code != 500

    # test collections/{id}/{year}/{country}
    U1 = BASE_URL + 'collections/1/2012/test'
    response = requests.get(U1)
    assert response.status_code != 500


def test_q1():
    """
    test q1
    """
    # method 1
    U1 = BASE_URL + 'collections?indicator_id=NY.GDP.MKTP.CD'
    response = requests.post(U1)
    assert response.status_code == 201
    
    # collect id
    data.append(response.json()['id'])

    # method 2
    U1 = BASE_URL + 'collections'
    response = requests.post(U1, params={'indicator_id': 'NY.GDP.MKTP.CD'})
    assert response.status_code != 201


def test_q2():
    """
    test q2
    """
    # random a id
    U1 = BASE_URL + 'collections/12312312'
    response = requests.delete(U1)
    assert response.status_code != 200
    assert response.status_code != 500

    # delete that
    ID = data.pop()
    U1 = BASE_URL + 'collections/{}'.format(ID)
    response = requests.delete(U1)
    assert response.status_code == 200

    # delete again
    U1 = BASE_URL + 'collections/{}'.format(ID)
    response = requests.delete(U1)
    assert response.status_code != 200
    assert response.status_code != 500


def test_q3():
    """
    test q3
    """
    # get a id
    U1 = BASE_URL + 'collections?indicator_id=NY.GDP.MKTP.CD'
    response = requests.post(U1)
    assert response.status_code == 201
    
    d1 = response.json()
    # collect id
    data.append(d1['id'])

    
    U1 = BASE_URL + 'collections'
    response = requests.get(U1)
    assert response.status_code == 200
    d2 = response.json()


    assert d1['uri'] == d2[0]['uri']
    assert d1['id'] == d2[0]['id']
    assert d1['creation_time'] == d2[0]['creation_time']
    assert d1['indicator_id'] == d2[0]['indicator']

    # add another indicator
    U1 = BASE_URL + 'collections?indicator_id=2.0.cov.Math.pl_3.all'
    response = requests.post(U1)
    assert response.status_code == 201

    d3 = response.json()
    data.append(d3['id'])

    # test error
    U1 = BASE_URL + 'collections?order_by=+xx'
    response = requests.get(U1)
    assert response.status_code != 200
    assert response.status_code != 500

    # test error
    U1 = BASE_URL + 'collections?order_by=+xx, +'
    response = requests.get(U1)
    assert response.status_code != 200
    assert response.status_code != 500

    # test error
    U1 = BASE_URL + 'collections?order_by=+xx,+yy'
    response = requests.get(U1)
    assert response.status_code != 200
    assert response.status_code != 500
    
    # test error
    U1 = BASE_URL + 'collections?order_by=asdas'
    response = requests.get(U1)
    assert response.status_code != 200
    assert response.status_code != 500

    # test error
    U1 = BASE_URL + 'collections?order_by=*asdas'
    response = requests.get(U1)
    assert response.status_code != 200
    assert response.status_code != 500


    # test error
    U1 = BASE_URL + 'collections?order_by=(asd)'
    response = requests.get(U1)
    assert response.status_code != 200
    assert response.status_code != 500

    # test error
    U1 = BASE_URL + 'collections?order_by==asd'
    response = requests.get(U1)
    assert response.status_code != 200
    assert response.status_code != 500
    
    # test orders
    U1 = BASE_URL + 'collections?order_by=%2Bid'
    response = requests.get(U1)
    assert response.status_code == 200

    d2 = response.json()
    
    assert len(d2) == 2
    assert int(d2[0]['id']) < int(d2[1]['id'])
    assert int(d2[0]['id']) != int(d2[1]['id'])


    # do not delete them
    


def test_q4():
    """
    test q4
    """

    # /collections/{id}
    U1 = BASE_URL + 'collections/{}'.format(data[0])
    response = requests.get(U1)
    assert response.status_code == 200
    assert response.status_code != 500

    assert response.json()['id'] == data[0]

    U1 = BASE_URL + 'collections/xx'
    response = requests.get(U1)
    assert response.status_code == 404


def test_q6():
    """
    test q6
    """

    # /collections/{id}
    U1 = BASE_URL + 'collections/{}/2012'.format(data[1])
    response = requests.get(U1)
    assert response.status_code == 200
    assert response.status_code != 500

    # check None
    d1 = response.json()
    for entry in d1['entries']:
        assert entry['value'] != None
    

    U1 = BASE_URL + 'collections/{}/2012?q=1000'.format(data[1])
    response = requests.get(U1)
    assert response.status_code != 200
    assert response.status_code != 500

    U1 = BASE_URL + 'collections/{}/2012?q=--1'.format(data[1])
    response = requests.get(U1)
    assert response.status_code != 200
    assert response.status_code != 500



    U1 = BASE_URL + 'collections/{}/2012?q=1'.format(data[1])
    response = requests.get(U1)
    assert response.status_code == 200
    assert response.status_code != 500

    d1 = response.json()

    assert len(d1['entries']) == 1


    U1 = BASE_URL + 'collections/{}/2012?q=2'.format(data[1])
    response = requests.get(U1)
    assert response.status_code == 200
    assert response.status_code != 500

    d1 = response.json()

    assert len(d1['entries']) == 2
    assert d1['entries'][0]['value'] > d1['entries'][1]['value']


    U1 = BASE_URL + 'collections/{}/2012?q=-2'.format(data[1])
    response = requests.get(U1)
    assert response.status_code == 200
    assert response.status_code != 500

    d1 = response.json()

    assert len(d1['entries']) == 2
    assert d1['entries'][0]['value'] < d1['entries'][1]['value']


    U1 = BASE_URL + 'collections/{}/2012?q=-xx'.format(data[1])
    response = requests.get(U1)
    assert response.status_code != 200
    assert response.status_code != 500

    U1 = BASE_URL + 'collections/{}/2012?q=xx'.format(data[1])
    response = requests.get(U1)
    assert response.status_code != 200
    assert response.status_code != 500

    U1 = BASE_URL + 'collections/{}/2012?q=+xx'.format(data[1])
    response = requests.get(U1)
    assert response.status_code != 200
    assert response.status_code != 500

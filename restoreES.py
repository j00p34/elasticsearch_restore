#!/usr/bin/env python

__author__ = 'zadelhoff'

import urllib2
import json

data = ""

method = "POST"

handler = urllib2.HTTPHandler()

opener = urllib2.build_opener(handler)


def openUrl(opener, request):
    global data
    try:
        connection = opener.open(request)
    except urllib2.HTTPError,e:
        connection = e

    # check. Substitute with appropriate HTTP code.
    if connection.code == 200:
        data = connection.read()
        return True
    else:
        data = connection.read()
        print data
        return False
        # handle the error case. connection.read() will still contain data
         # if any was returned, but it probably won't be of any use

def close( index ):
    url = 'http://localhost:9200/' + index + '/_close'
    request = urllib2.Request(url, data=None)
    # overload the get method function with a small anonymous function...
    request.get_method = lambda: method
    openUrl(opener, request)


request0 = urllib2.Request('http://localhost:9200/_stats/indexes?pretty', data=None)
openUrl(opener, request0)

for k in json.loads(data)['indices']:
    close(k)

request3 = urllib2.Request('http://localhost:9200/_snapshot/prod_s3_repository/backup/_restore', data=None)
request3.get_method = lambda: method

if (openUrl(opener, request3)):
    with open('/var/tmp/ESrestoreStatus', 'w') as f:
        f.write(data)
else:
    with open('/var/log/elasticsearch/restore.log', 'a') as f:
        f.write(data + "\n")




#!/usr/bin/env python

__author__ = 'zadelhoff'

import urllib2

data = ""

method = "POST"

handler = urllib2.HTTPHandler()

opener = urllib2.build_opener(handler)

def openUrl(opener, request):

    try:
        connection = opener.open(request)
    except urllib2.HTTPError,e:
        connection = e

    # check. Substitute with appropriate HTTP code.
    if connection.code == 200:
        global data
        data = connection.read()
        return True
    else:
        global data
        data = connection.read()
        print data
        return False
        # handle the error case. connection.read() will still contain data
         # if any was returned, but it probably won't be of any use

request1 = urllib2.Request('http://localhost:9200/contentrepo-2014-09-02/_open', data=None)
# overload the get method function with a small anonymous function...
request1.get_method = lambda: method

request2 = urllib2.Request('http://localhost:9200/contentrepo-2014-09-23/_open', data=None)
request2.get_method = lambda: method

request3 = urllib2.Request('http://localhost:9200/_snapshot/prod_s3_repository/backup/_restore', data=None)
request3.get_method = lambda: method

print openUrl(opener, request1)
print openUrl(opener, request2)

if (openUrl(opener, request3)):
    with open('/var/tmp/ESrestoreStatus', 'w') as f:
        f.write(data)
else:
    print data
    with open('/var/log/elasticsearch/restore.log', 'w') as f:
        f.write(data)


#!/usr/bin/env python

__author__ = 'zadelhoff'

import urllib2
import json
from datetime import datetime

method = "POST"

handler = urllib2.HTTPHandler()

opener = urllib2.build_opener(handler)


def openUrl(opener, request):
    try:
        connection = opener.open(request)
    except urllib2.HTTPError, e:
        connection = e

    # check. Substitute with appropriate HTTP code.
    if connection.code == 200:
        data = connection.read()
        return True, data
    else:
        data = connection.read()
        return False, data
        # handle the error case. connection.read() will still contain data
        # if any was returned, but it probably won't be of any use


def close(index):
    url = 'http://localhost:9200/' + index + '/_close'
    request = urllib2.Request(url, data=None)
    # overload the get method function with a small anonymous function...
    request.get_method = lambda: method
    openUrl(opener, request)


request0 = urllib2.Request('http://localhost:9200/_stats/indexes?pretty', data=None)

indexes = openUrl(opener, request0)
if (indexes[0]):
    for k in json.loads(indexes[1])['indices']:
        close(k)

    restoreRequest = urllib2.Request('http://localhost:9200/_snapshot/prod_s3_repository/backup/_restore', data=None)
    restoreRequest.get_method = lambda: method

    restoreResult = openUrl(opener, restoreRequest)

    if (restoreResult[0]):
        with open('/var/tmp/ESrestoreStatus', 'w') as f:
            f.write(restoreResult[1])
    else:
        with open('/var/log/elasticsearch/restore.log', 'a') as f:
#        with open('/tmp/restore.log', 'a') as f:
            f.write(datetime.now() + restoreResult[1] + "\n")
else:
    with open('/var/log/elasticsearch/restore.log', 'a') as f:
        f.write(indexes[1] + "\n")




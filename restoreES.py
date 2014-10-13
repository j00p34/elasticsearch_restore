#!/usr/bin/env python

__author__ = 'zadelhoff'

import urllib2
import json
from datetime import datetime

method = "POST"

handler = urllib2.HTTPHandler()

opener = urllib2.build_opener(handler)


def openUrl(opener, request):
    #Returns (True, return data) if reuest succeeeds
    #(False, return data) if anything else
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
        # if any was returned, useful for error logging


def close(index):
    #closes index
    url = 'http://localhost:9200/' + index + '/_close'
    request = urllib2.Request(url, data=None)
    # overload the get method function with a small anonymous function...
    request.get_method = lambda: method
    openUrl(opener, request)

request0 = urllib2.Request('http://localhost:9200/_snapshot/prod_s3_repository/_all?pretty', data=None)

snapnum = -1
snapshots = openUrl(opener, request0)
while (snapnum != 0):
    if (json.loads(snapshots[1])['snapshots'][snapnum]['state'] == 'SUCCESS'):
        snapname = json.loads(snapshots[1])['snapshots'][-1]['snapshot']
        print 'success'
        break
    else: snapnum -= 1

#get all indexes
request1 = urllib2.Request('http://localhost:9200/_stats/indexes?pretty', data=None)

#close all indexes
indexes = openUrl(opener, request1)
if (indexes[0]): 
    #indexes[0] = True if http request returns 200
    for k in json.loads(indexes[1])['indices']: 
        #keys in indexes[1]['indices'] are names of indexes
        close(k)

    #Restore snapshot
    restoreRequest = urllib2.Request('http://localhost:9200/_snapshot/prod_s3_repository/' + snapname + '/_restore', data=None)
    restoreRequest.get_method = lambda: method

    restoreResult = openUrl(opener, restoreRequest)

    #Log return data to file if request succeeds
    if (restoreResult[0]):
        with open('/var/tmp/ESrestoreStatus', 'w') as f:
            f.write(restoreResult[1])
    else:
        #If request fails, log Error
        with open('/var/log/elasticsearch/restore.log', 'a') as f:
#        with open('/tmp/restore.log', 'a') as f:
            f.write(datetime.now() + restoreResult[1] + "\n")
else:
    #If closing of indexes fails, log Error
    with open('/var/log/elasticsearch/restore.log', 'a') as f:
        f.write(indexes[1] + "\n")




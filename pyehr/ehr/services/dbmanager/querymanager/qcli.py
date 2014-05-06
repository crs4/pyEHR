import sys
import os
import ConfigParser
from pyehr.aql.model import *

import httplib2 as http
from urllib import urlencode
import json

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

class QueryClient():

    def __init__(self):
        self.config = None

    def executeQuery(self, queryStatement, versionTime=None):
        pass

    def main(self, argv=None):
        try:
            self.executeQuery('')
        except Exception, e:
            print "Error: %s" % str(e)

    def load_configuration(self, fileconf):
        try:
            if os.path.exists(fileconf):
                self.config = ConfigParser.RawConfigParser()
                self.config.read(fileconf)
            else:
                print "The %s configuration file does not exist." % fileconf
                sys.exit(1)
        except:
            print "Error opening the %s configuration file." % fileconf
            sys.exit(1)

    def connect(self, auth=None, aql=None):
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json; charset=UTF-8'
        }

        uri = self.config.get('service', 'host')
        port = self.config.get('service', 'port')
        queryPath = '/query'

        #aql = "SELECT TOP 10 c/name/value AS Name, c/context/start_time AS date_time, c/composer/name AS Composer FROM EHR e[ehr_id/value=$ehrUid] CONTAINS COMPOSITION c"

        target = urlparse('http://'+uri+':'+port+queryPath)
        method = 'POST'
        body = ''
        data = dict(name='Some Name', query=aql)
        h = http.Http()
        # If you need authentication some example:
        if auth:
            h.add_credentials(auth.user, auth.password)
        response, content = h.request(target.geturl(),method,urlencode(data),headers)
        print ""
        print "QueryService Response: " + content
        print ""

    def main(self, argv):
        try:
            self.load_configuration(argv[1])
            self.connect(aql=argv[2])
        except Exception, e:
            print "Error: %s" % str(e)

if __name__ == '__main__':
    qc = QueryClient()
    qc.main(sys.argv)


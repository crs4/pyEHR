import sys
import os
import ConfigParser
import json
import pyehr
from pyehr.aql.parser import *
from pyehr.ehr.services.dbmanager.querymanager.query import *
from pyehr.ehr.services.dbmanager import *

from bottle import route, run, Response, request

class QueryService():

    def __init__(self):
        self.parser = Parser()
        self.dbdriver = None
        self.config = None
        ###############################################
        # Web Service methods
        ###############################################
        route('/query', method='POST')(self.query)

    def query(self):
        params = request.forms
        resp = None
        if 'query' in params:
            try:
                query = params.get('query')
                obj = self.parseQuery(query)
                rv = self.queryDB(obj)
                resp = Response(body=json.dumps(rv))
            except ParsingError as pe:
                print "ParsingError Error: %s" % str(pe)
                resp = Response(body="An error occurred while parsing the AQL query: %s" % str(pe))
            except ParseSelectionError as pe:
                print "ParseSelectionError Error: %s" % str(pe)
                resp = Response(body="An error occurred while parsing the SELECTION statement of the AQL query: " % str(pe))
            except ParseLocationError as pe:
                print "ParseLocationError Error: %s" % str(pe)
                resp = Response(body="An error occurred while parsing the LOCATION statement of the AQL query: " % str(pe))
            except ParseConditionError as pe:
                print "ParseConditionError Error: %s" % str(pe)
                resp = Response(body="An error occurred while parsing the CONDITION statement of the AQL query: " % str(pe))
            except ParseOrderRulesError as pe:
                print "ParseOrderRulesError Error: %s" % str(pe)
                resp = Response(body="An error occurred while parsing the ORDER_RULES statement of the AQL query: " % str(pe))
            except ParseTimeConstraintsError as pe:
                print "ParseTimeConstraintsError Error: %s" % str(pe)
                resp = Response(body="An error occurred while parsing the TIME_CONSTRAINTS statement of the AQL query: " % str(pe))
            except InvalidAQLError as e:
                print "InvalidAQLError Error: %s" % str(e)
                resp = Response(body="Invalid AQL: " % str(e))
            except Exception as e:
                print "Exception Error: %s" % str(e)
                resp = Response(body='An Error Occurred: ' % str(e))
        else:
            resp = Response(body="Method not supported")
        return resp

    def parseQuery(self, queryStatement, versionTime=None):
        obj = self.parser.parse(queryStatement)
        return obj

    def queryDB(self, query):
        rs = self.dbdriver.execute_query(query)
        if rs:
            return rs.get_json()
        else:
            return None

    def load_configuration(self, fileconf):
        try:
            if os.path.exists(fileconf):
                self.config = ConfigParser.RawConfigParser()
                self.config.read(fileconf)
                # Service configuration
                self.host = self.config.get('service', 'host')
                self.port = self.config.get('service', 'port')
                # DB configuration
                driver = None
                host = None
                database = None
                port = None
                collection = None
                user = None
                passwd = None
                if self.config.has_option('db', 'driver'):
                    driver = self.config.get('db', 'driver')
                else:
                    self.forceExit("ERROR: The driver option is missing in the configuration file")
                if self.config.has_option('db', 'host'):
                    host = self.config.get('db', 'host')
                else:
                    self.forceExit("ERROR: The host option is missing in the configuration file")
                if self.config.has_option('db', 'database'):
                    database = self.config.get('db', 'database')
                else:
                    self.forceExit("ERROR: The database option is missing in the configuration file")
                if self.config.has_option('db', 'port'):
                    port = int(self.config.get('db', 'port'))
                else:
                    self.forceExit("ERROR: The port option is missing in the configuration file")
                if self.config.has_option('db', 'collection'):
                    collection = self.config.get('db', 'collection')
                if self.config.has_option('db', 'user'):
                    user = self.config.get('db', 'user')
                if self.config.has_option('db', 'passwd'):
                    passwd = self.config.get('db', 'passwd')
                paramList = (host,database,port,collection,user,passwd)
                self.dbdriver = build_driver(driver,paramList)
            else:
                print "The %s configuration file does not exist." % fileconf
                sys.exit(1)
        except Exception as e:
            print "Error: %s" % str(e)
            print "Error opening the %s configuration file." % fileconf
            sys.exit(1)

    def forceExit(self, msg):
        print msg
        sys.exit(1)

    def main(self, argv):
        print("Starting the QueryService daemon...")
        try:
            self.load_configuration(argv[1])
            run(host=self.host, port=self.port, debug=True)
        except Exception, e:
            print "Error: %s" % str(e)

if __name__ == '__main__':
    qs = QueryService()
    qs.main(sys.argv)

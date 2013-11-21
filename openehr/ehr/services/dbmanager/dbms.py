__author__ = 'ciccio'

import sys
import threading

from openehr.ehr.services.dbmanager.querymanager.queryservice import *

class DBMS():
    def __init__(self):
        self.queryservice = QueryService()

    def run(self):
        print "Starting the DBMS thread..."
        while True:
            pass

    def runqs(self):
        print "Starting the QS thread..."
        #self.queryservice.clap()
        #self.queryservice.run()

    def main(self, argv=None):
        try:
            dbm_thread = threading.Thread(name="dbm", target=self.run(), args=None, kwargs=None)
            dbm_thread.setDaemon(True)
            qs_thread = threading.Thread(name="qs", target=self.runqs(), args=None, kwargs=None)
            qs_thread.setDaemon(True)
            qs_thread.start()
            dbm_thread.start()
        except Exception, e:
            print "Error: %s" % str(e)

if __name__ == '__main__':
    dbms = DBMS()
    dbms.main(sys.argv)

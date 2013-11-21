from openehr.ehr.services.dbmanager.drivers.mongo import *

def build_driver(kb,paramList):
    def build_mongo_driver(paramList):
        host=paramList[0]
        database=paramList[1]
        port=paramList[2]
        collection=paramList[3]
        user=paramList[4]
        passwd=paramList[5]
        d = MongoDriver(host,database,collection,port=port,user=user,passwd=passwd)
        return d

    drivers_map = {
        'mongo': build_mongo_driver,
    }

    return drivers_map[kb](paramList)

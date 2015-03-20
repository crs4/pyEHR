import json
import random
from pyehr.ehr.services.dbmanager.dbservices import DBServices
from pyehr.utils.services import get_service_configuration
from pyehr.ehr.services.dbmanager.dbservices.wrappers import ClinicalRecord, PatientRecord
from pyehr.ehr.services.dbmanager.dbservices import DBServices

from structures_builder import build_record

def run():
    archetype_dir = '/home/luca/work/pyEHR/models/json'

    paths = [
            ['composition.lbl-00007', 'composition.lbl-00010', 'composition.lbl-00002',
             'composition.lbl-00012', 'blood_pressure'],
            ['composition.lbl-00014', 'composition.lbl-00018', 'composition.lbl-00016',
             'composition.lbl-00011', 'blood_pressure'],
            ['composition.lbl-00017', 'composition.lbl-00012', 'composition.lbl-00013',
             'composition.lbl-00007', 'urin_analysis'],
            ['composition.lbl-00002', 'composition.lbl-00009', 'composition.lbl-00015',
             'composition.lbl-00010', 'urin_analysis'],
        ]
    with open('matching_structure_file.json') as f:
        structures = json.loads(f.read())

    crs = []
    for index, p in enumerate(paths):
        for i in xrange(100):
            a = build_record(structures[index], archetype_dir, True, p[-1])
            cr = ClinicalRecord(a)
            crs.append(cr)

    return crs

def save_on_db(crs):
    cfg = get_service_configuration('/home/luca/work/pyEHR/config/bruja.mongodb.conf')
    dbcfg = cfg.get_db_configuration()
    dbcfg['database'] = 'pyehr_d-4_w-10_str-2000'
    dbs = DBServices(**dbcfg)
    icfg = cfg.get_index_configuration()
    icfg['database'] = 'pyehr_index_d-4_w-10_str-2000'
    dbs.set_index_service(**icfg)

    print len(crs)
    for x in xrange(len(crs)):
        print 'PATIENT_%05d' % (x + 30000)
        p = dbs.save_patient(PatientRecord('PATIENT_%05d' % (x + 30000)))
        print "Patient retrieved"
        dbs.save_ehr_record(crs[x], p)
        print "saved"

crs = run()
save_on_db(crs)

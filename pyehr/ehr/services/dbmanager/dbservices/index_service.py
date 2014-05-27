from lxml import etree
from hashlib import md5
from uuid import uuid4
import BaseXClient


class IndexService(object):

    def __init__(self, db, host, port, user, passwd):
        self.host = host
        self.port = int(port) # just to be safe
        self.user = user
        self.passwd = passwd
        self.db = db
        self.session = None

    def connect(self):
        self.session = BaseXClient.Session(self.host, self.port, self.user, self.passwd)
        self.session.execute('check %s' % self.db)

    def disconnect(self):
        self.session.close()
        self.session = None

    @staticmethod
    def get_structure(ehr_record):
        def is_archetype(doc):
            return 'archetype' in doc
        # TODO: sort records structure?
        root = etree.Element('archetype', {'class': ehr_record['archetype']})
        for x in ehr_record['ehr_data'].values():
            if type(x) == dict and is_archetype(x):
                root.append(IndexService.get_structure(x))
        return root

    def _get_record_hash(self, record):
        record_hash = md5()
        record_hash.update(etree.tostring(record))
        return record_hash.hexdigest()

    def _build_new_record(self, record):
        record_hash = self._get_record_hash(record)
        record_id = uuid4().hex
        record.append(etree.Element('references_counter', {'hits': '1'}))
        record.append(etree.Element('structure_id', {'str_hash': record_hash,
                                                     'uid': record_id}))
        return record, record_id

    def create_entry(self, record):
        record, structure_key = self._build_new_record(record)
        if not self.session:
            self.connect()
        self.session.add('path_index', etree.tostring(record))
        return structure_key

    def _get_structure_id(self, xml_doc):
        if not self.session:
            self.connect()
        record_hash = self._get_record_hash(xml_doc)
        query = self.session.query('collection("%s")/archetype/structure_id[@str_hash="%s"]' %
                                   (self.db, record_hash))
        res = query.execute()
        if len(res) == 0:
            return None
        else:
            return etree.fromstring(res).get('uid')

    def get_structure_id(self, ehr_record):
        """
        Return the STRUCTURE_ID related to the given EHR, if no ID is related to
        record's structure create a new entry in the DB and return the newly created
        value

        :param ehr_record: the EHR as a dictionary
        :type ehr_record: dictionary
        """
        if not self.session:
            self.connect()
        xml_structure = IndexService.get_structure(ehr_record)
        str_id = self._get_structure_id(xml_structure)
        if not str_id:
            str_id = self.create_entry(xml_structure)
        self.disconnect()
        return str_id
from lxml import etree
from hashlib import md5
from uuid import uuid4
from pyehr.utils.services import get_logger
from pyehr.libs.python import BaseXClient
from socket import error as serror
from pyehr.ehr.services.dbmanager.errors import IndexServiceConnectionError


class IndexService(object):

    def __init__(self, db, host, port, user, passwd, logger=None):
        self.host = host
        self.port = int(port) # just to be safe
        self.user = user
        self.passwd = passwd
        self.db = db
        self.session = None
        self.logger = logger or get_logger('index_service')

    def connect(self):
        try:
            self.session = BaseXClient.Session(self.host, self.port, self.user, self.passwd)
        except serror:
            self.logger.error('Unable to connect to BaseX server at %s:%d', self.host,
                              self.port)
            raise IndexServiceConnectionError('Unable to connect to Index service at %s:%d' %
                                              (self.host, self.port))
        self.session.execute('check %s' % self.db)

    def disconnect(self):
        self.session.close()
        self.session = None

    def _execute_query(self, xpath_query):
        if not self.session:
            self.connect()
        q = self.session.query(xpath_query)
        res = '<results>%s</results>' % q.execute().replace('\n', '')
        return etree.fromstring(res)

    @staticmethod
    def get_structure(ehr_record, parent_key=[]):
        def is_archetype(doc):
            return 'archetype_class' in doc

        def build_path(keys_list):
            if len(keys_list) == 0:
                return '/'
            path = str()
            for k in keys_list:
                if k.startswith('at'):
                    path += '[%s]' % k
                else:
                    path += '/%s' % k
            return path

        def get_structure_from_dict(doc, parent_key):
            archetypes = []
            for k, v in sorted(doc.iteritems()):
                pk = parent_key + [k]
                if isinstance(v, dict):
                    if is_archetype(v):
                        archetypes.append(IndexService.get_structure(v, pk))
                    else:
                        a_from_dict = get_structure_from_dict(v, pk)
                        if len(a_from_dict) > 0:
                            archetypes.extend(a_from_dict)
                if isinstance(v, list):
                    a_from_list = get_structure_from_list(v, pk)
                    if len(a_from_list) > 0:
                        archetypes.extend(a_from_list)
            return archetypes

        def get_structure_from_list(dlist, parent_key):
            def list_sort_key(element):
                if is_archetype(element):
                    return element['archetype_class']
                else:
                    return element

            archetypes = []
            for x in sorted(dlist, key=list_sort_key):
                if isinstance(x, dict):
                    if is_archetype(x):
                        archetypes.append(IndexService.get_structure(x, parent_key))
                    else:
                        a_from_dict = get_structure_from_dict(x, parent_key)
                        if len(a_from_dict) > 0:
                            archetypes.extend(a_from_dict)
                if isinstance(x, list):
                    a_from_list = get_structure_from_list(x, parent_key)
                    if len(a_from_list) > 0:
                        archetypes.extend(a_from_list)
            return archetypes

        # TODO: sort records structure?
        root = etree.Element(
            'archetype',
            {'class': ehr_record['archetype_class'], 'path_from_parent': build_path(parent_key)}
        )
        parent_key = []
        for k, x in sorted(ehr_record['archetype_details'].iteritems()):
            pk = parent_key + [k]
            if isinstance(x, dict):
                if is_archetype(x):
                    root.append(IndexService.get_structure(x, pk))
                else:
                    for a in get_structure_from_dict(x, pk):
                        root.append(a)
            if isinstance(x, list):
                for a in get_structure_from_list(x, pk):
                    root.append(a)
        return root

    def _get_record_hash(self, record):
        record_hash = md5()
        record_hash.update(etree.tostring(record))
        return record_hash.hexdigest()

    def _build_new_record(self, record):
        record_root = etree.Element('archetype_structure')
        record_root.append(record)
        record_hash = self._get_record_hash(record)
        record_id = uuid4().hex
        record_root.append(etree.Element('references_counter', {'hits': '1'}))
        record_root.append(etree.Element('structure_id', {'str_hash': record_hash,
                                                          'uid': record_id}))
        return record_root, record_id

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
        res = self._execute_query('collection("%s")/archetype_structure/structure_id[@str_hash="%s"]' %
                                  (self.db, record_hash))
        try:
            return res.find('structure_id').get('uid')
        except AttributeError:
            return None

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

    def _container_to_xpath(self, aql_container):
        if aql_container.class_expression.predicate:
            archetype_class = aql_container.class_expression.predicate.archetype_id
        else:
            #TODO: maybe using the ReferenceModel can help to map generic Archetypes
            archetype_class = None
        if archetype_class:
            return 'archetype[@class="%s"]' % archetype_class
        else:
            return 'archetype'

    def _build_xpath_query(self, aql_containers):
        # Right now, the AQLParsers maps CONTAIN statements into a list where
        # cont[n] contains cont[n+1]
        xpath_queries = [self._container_to_xpath(c) for c in aql_containers]
        query = 'collection("%s")/archetype_structure' % self.db
        for xpq in xpath_queries:
            query += '//%s' % xpq
        query += '/ancestor-or-self::archetype_structure'
        return query

    def _get_matching_ids(self, results):
        return [(a.find('structure_id').get('uid')) for a in results.findall('archetype_structure')]

    def _get_matching_paths(self, results, archetype_class):
        paths = list()
        for node in results.xpath('//archetype[@class="{0}"]/self::*[@class="{0}"]'.format(archetype_class)):
            path = list()
            path.append(node.get('path_from_parent'))
            root = node.getroottree()
            while node.getparent() != root and len(node.getparent()):
                node = node.getparent()
                if node.get('path_from_parent'):
                    path.insert(0, node.get('path_from_parent'))
                else:
                    break
            paths.append(tuple(path))
        return paths

    def map_aql_contains(self, aql_containers):
        """
        Return the list of STRUCTURE_IDs related to all ADL structures that
        match the given AQL containment statements

        :param aql_containers: the containers list generated by the AQLParser
        """
        if not self.session:
            self.connect()
        query = self._build_xpath_query(aql_containers)
        res = self._execute_query(query)
        self.disconnect()
        structure_ids = self._get_matching_ids(res)
        path_mappings = dict()
        for c in aql_containers:
            if c.class_expression.predicate:
                path_mappings[c.class_expression.variable_name] = \
                    self._get_matching_paths(res, c.class_expression.predicate.archetype_id)
        return structure_ids, path_mappings
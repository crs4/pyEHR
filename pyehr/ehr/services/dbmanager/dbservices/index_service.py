from lxml import etree
from hashlib import md5
from uuid import uuid4
from copy import copy
from pyehr.utils.services import get_logger
from pybasex import BaseXClient
import pybasex.errors as pbx_errors


class IndexService(object):

    def __init__(self, db, url, user, passwd, logger=None):
        self.url = url
        self.user = user
        self.passwd = passwd
        self.db = db
        self.basex_client = None
        self.logger = logger or get_logger('index_service')

    def connect(self):
        self.basex_client = BaseXClient(self.url, self.db, self.user, self.passwd, self.logger)
        self.basex_client.connect()
        try:
            self.basex_client.create_database()
        except pbx_errors.OverwriteError:
            # DB already exists, just ignore
            pass

    def disconnect(self):
        self.basex_client.disconnect()
        self.basex_client = None

    def _execute_query(self, xpath_query):
        if not self.basex_client:
            self.connect()
        res = self.basex_client.execute_query(xpath_query)
        return res

    @staticmethod
    def get_structure(ehr_record, parent_key=None):
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
                        structure = IndexService.get_structure(x, parent_key)
                        if etree.tostring(structure) not in [etree.tostring(a) for a in archetypes]:
                            archetypes.append(structure)
                    else:
                        a_from_dict = get_structure_from_dict(x, parent_key)
                        if len(a_from_dict) > 0:
                            archetypes.extend(a_from_dict)
                if isinstance(x, list):
                    a_from_list = get_structure_from_list(x, parent_key)
                    if len(a_from_list) > 0:
                        archetypes.extend(a_from_list)
            return archetypes

        if parent_key is None:
            parent_key = []
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

    def _build_new_record(self, record, record_id=None):
        record_root = etree.Element('archetype_structure')
        record_root.append(record)
        record_hash = self._get_record_hash(record)
        record_id = record_id or uuid4().hex
        # new records are created with a reference counter set to 0, only when
        # the reference counter will be increased only after the record will
        # actually be saved on the DB
        record_root.append(etree.Element('references_counter', {'hits': '0'}))
        record_root.append(etree.Element('structure_id', {'str_hash': record_hash,
                                                          'uid': record_id}))
        return record_root, record_id

    def create_entry(self, record, record_id=None):
        record, structure_key = self._build_new_record(record, record_id)
        if not self.basex_client:
            self.connect()
        self.basex_client.add_document(record, structure_key)
        return structure_key

    def _get_structure_by_id(self, structure_id):
        if not self.basex_client:
            self.connect()
        return self.basex_client.get_document(structure_id)

    def _extract_structure_id_from_xml(self, xml_doc):
        return xml_doc.find('structure_id').get('uid')

    def _get_structure_id(self, xml_doc):
        if not self.basex_client:
            self.connect()
        record_hash = self._get_record_hash(xml_doc)
        res = self._execute_query('/archetype_structure/structure_id[@str_hash="%s"]' % record_hash)
        try:
            return self._extract_structure_id_from_xml(res)
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
        if not self.basex_client:
            self.connect()
        xml_structure = IndexService.get_structure(ehr_record)
        str_id = self._get_structure_id(xml_structure)
        if not str_id:
            str_id = self.create_entry(xml_structure)
        self.disconnect()
        return str_id

    def _get_document_reference_counter(self, doc):
        return int(doc.find("references_counter").get("hits"))

    def _update_document_references_counter(self, doc, update_value):
        doc.find("references_counter").set("hits", str(update_value))
        return doc

    def check_structure_counter(self, structure_id):
        """
        Check if a structure with ID *structure_id* has a references counter equal to 0.
        If so, delete the structure because it is not referenced by a clinical record.

        :param structure_id: the ID of the structure that will be checked
        """
        doc = self._get_structure_by_id(structure_id)
        if doc is not None:
            doc_count = self._get_document_reference_counter(doc)
            if doc_count == 0:
                self.basex_client.delete_document(structure_id)
            else:
                self.logger.debug("References counter for structure %s id %d",
                                  doc_count, structure_id)

    def increase_structure_counter(self, structure_id, increase_value=1):
        """
        Increase the value of the references counter of the structure with the
        given *structure_id* by the amount specified by *increase_value*.

        :param structure_id: the ID of the structure
        :param increase_value: the value that will be added to structure's references counter
        """
        if increase_value < 1:
            raise ValueError("increase_value must be an integer greater than 0")
        doc = self._get_structure_by_id(structure_id)
        if doc is not None:
            doc_count = self._get_document_reference_counter(doc)
            self.logger.debug("Current counter for %s is %d", structure_id, doc_count)
            doc = self._update_document_references_counter(doc, (doc_count + increase_value))
            self.basex_client.delete_document(structure_id)
            self.basex_client.add_document(doc, structure_id)
            self.logger.debug("Documents %s updated", structure_id)
        else:
            self.logger.warn("There is no document with structure ID %s", structure_id)

    def decrease_structure_counter(self, structure_id, decrease_value=1):
        """
        Decrease the value of the references counter of the structure with the
        given *structure_id* by the amount specified by *decrease_value*.
        If references counter reaches a value equal or lower than 0, the
        structure will be delete.

        :param structure_id: the ID of the structure
        :param decrease_value: the value that will be subtracted from structure's references counter
        """
        if decrease_value < 1:
            raise ValueError("decrease_value must be an integer greater than 0")
        doc = self._get_structure_by_id(structure_id)
        if doc is not None:
            doc_count = self._get_document_reference_counter(doc)
            if (doc_count - decrease_value) <= 0:
                self.basex_client.delete_document(structure_id)
            else:
                doc = self._update_document_references_counter(doc, (doc_count - decrease_value))
                self.basex_client.delete_document(structure_id)
                self.basex_client.add_document(doc, structure_id)
                self.logger.debug("Document %s updated", structure_id)
        else:
            self.logger.warn("There is no document with structure ID %s", structure_id)

    def _container_to_xpath(self, aql_container):
        if aql_container.class_expression.predicate:
            archetype_class = aql_container.class_expression.predicate.archetype_id
        else:
            # TODO: maybe using the ReferenceModel can help to map generic Archetypes
            archetype_class = None
        if archetype_class:
            return 'archetype[@class="%s"]' % archetype_class
        else:
            return 'archetype'

    def _build_xpath_query(self, aql_containers):
        # Right now, the AQLParsers maps CONTAIN statements into a list where
        # cont[n] contains cont[n+1]
        xpath_queries = '//'.join([self._container_to_xpath(c) for c in aql_containers])
        query = '/archetype_structure//%s/ancestor-or-self::archetype_structure' % xpath_queries
        return query

    def _get_matching_ids(self, results):
        return [(a.find('structure_id').get('uid')) for a in results.findall('archetype_structure')]

    def _resolve_node_paths(self, node, container_classes, leaf_class):
        paths_map = dict()
        paths_map.setdefault(leaf_class, []).insert(0, node.get('path_from_parent'))
        while len(container_classes):
            current_class = container_classes.pop(-1)
            while len(node.getparent()) and node.get('class') != current_class:
                node = node.getparent()
                for v in paths_map.values():
                    v.insert(0, node.get('path_from_parent'))
            paths_map.setdefault(current_class, []).append(node.get('path_from_parent'))
        # container_classes mapped, go back and complete all paths, if necessary
        while len(node.getparent()) and node.tag != 'archetype_structure':
            node = node.getparent()
            if node.get('path_from_parent'):
                for v in paths_map.values():
                    v.insert(0, node.get('path_from_parent'))
        return node.find('structure_id').get('uid'), paths_map

    def map_aql_contains(self, aql_containers):
        if not self.basex_client:
            self.connect()
        query = self._build_xpath_query(aql_containers)
        res = self._execute_query(query)
        self.disconnect()
        structures_map = dict()
        variables_map = dict((c.class_expression.variable_name, c.class_expression.predicate.archetype_id)
                             for c in aql_containers if c.class_expression.predicate)
        container_classes = [c.class_expression.predicate.archetype_id
                             for c in aql_containers if c.class_expression.predicate]
        leaf_node = container_classes.pop(-1)
        # Look for nodes that match path described by aql_containers in res and get a reference to leaf_node nodes
        ln_query_path = '//'.join([self._container_to_xpath(c) for c in aql_containers])
        ln_query = '//archetype_structure//%s/self::*[@class="%s"]' % (ln_query_path, leaf_node)
        for node in res.xpath(ln_query):
            str_id, paths_map = self._resolve_node_paths(node, copy(container_classes), leaf_node)
            structures_map.setdefault(str_id, []).append(paths_map)
        return structures_map, variables_map
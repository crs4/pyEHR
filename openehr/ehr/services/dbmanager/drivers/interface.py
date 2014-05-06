from abc import ABCMeta, abstractmethod


class DriverInterface(object):
    """
    This abstract class acts as an interface for all the driver classes
    implemented to provide database services
    """
    __metaclass__ = ABCMeta

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.disconnect()
        return None

    @abstractmethod
    def connect(self):
        """
        Open a connection to the backend server
        """
        pass

    @abstractmethod
    def disconnect(self):
        """
        Close the connection to the backend server
        """
        pass

    @abstractmethod
    def init_structure(self, structure_def):
        """
        If needed, create a new data structure in the backend server defined
        by structure_def
        """
        pass

    @abstractmethod
    def executeQuery(self, query):
        pass

    @abstractmethod
    def encode_record(self, record):
        """
        Encode a :class:`Record` into a valid structure for the backend server
        """
        pass

    @abstractmethod
    def decode_record(self, record):
        """
        Encode a data structure coming from the backend server into a :class:`Record`
        object
        """
        pass

    @abstractmethod
    def add_record(self, record):
        """
        Add a record in the backend server
        """
        pass

    @abstractmethod
    def add_records(self, records):
        """
        Add a list of records in the backed server
        """
        return [self.add_record(r) for r in records]

    @abstractmethod
    def get_record_by_id(self, record_id):
        """
        Retrieve a record by giving a record ID
        """
        pass

    @abstractmethod
    def get_all_records(self):
        """
        Retrieve all records from the backed server
        """
        pass

    @abstractmethod
    def delete_record(self, record_id):
        """
        Delete a record from the backend server by giving the record ID
        """
        pass

    @abstractmethod
    def update_field(self, record_id, field_label, field_value,
                     update_timestamp_label):
        """
        Update the field with label *field_label* of the record with ID *record_id* with the
        value provided as *field_value* and update timestamp in field *update_timestamp_label*
        """
        pass

    @abstractmethod
    def add_to_list(self, record_id, list_label, item_value,
                    update_timestamp_label):
        """
        Add the value provided with the *item_value* field to the list with label *list_label*
        of the record with ID *record_id* and update the timestamp in field *update_timestamp_label*
        """
        pass

    @abstractmethod
    def remove_from_list(self, record_id, list_label, item_value,
                         update_timestamp_label):
        """
        Remove the value provided with the *item_value* field from the list with label *list_label*
        of the record with ID *record_id* and update the timestamp in field *update_timestamp_label*
        """
        pass

from pyehr.ehr.services.dbmanager.drivers.factory import DriversFactory
from pyehr.utils import get_logger
from pyehr.ehr.services.dbmanager.dbservices.index_service import IndexService
from pyehr.aql.parser import Parser


class QueryManager(object):
    """
    TODO: add documentation here
    """

    def __init__(self, driver, host, database, versioning_database=None,
                 patients_repository=None, ehr_repository=None,
                 ehr_versioning_repository=None, port=None, user=None,
                 passwd=None, logger=None):
        self.driver = driver
        self.host = host
        self.database = database
        self.versioning_manager = versioning_database
        self.patients_repository = patients_repository
        self.ehr_repository = ehr_repository
        self.ehr_versioning_repository = ehr_versioning_repository
        self.port = port
        self.user = user
        self.passwd = passwd
        self.index_service = None
        self.logger = logger or get_logger('query_manager')

    def _get_drivers_factory(self, repository):
        return DriversFactory(
            driver=self.driver,
            host=self.host,
            database=self.database,
            repository=repository,
            port=self.port,
            user=self.user,
            passwd=self.passwd,
            index_service=self.index_service,
            logger=self.logger
        )

    def set_index_service(self, url, database, user, passwd):
        """
        Add a :class:`IndexService` to the current :class:`QueryManager` that will be used
        to index clinical records
        :param url: the host of the :class:`IndexService`
        :type url: str
        :param database: the database used to store the indices
        :type database: str
        :param user: the user to access the :class:`IndexService`
        :type user: str
        :param passwd: the password to access the :class:`IndexService`
        :type passwd: str
        """
        self.index_service = IndexService(database, url, user, passwd, self.logger)

    def execute_aql_query(self, query, query_params=None, count_only=False, query_processes=1):
        """
        Execute an AQL query and return a :class:`pyehr.ehr.services.dbmanager.querymanager.results_wrappers.ResultSet`
        object that maps the obtained results.
        If the query has one or more parameters, they will be passed using query_params field.

        :param query: an AQL query
        :type query: str
        :param query_params: a dictionary containing query parameters as keys and their values
        :type query_params: dict
        :return: a :class:`pyehr.ehr.services.dbmanager.querymanager.results_wrappers.ResultSet` object
        """
        if query_params:
            if not isinstance(query_params, dict):
                raise ValueError('query_params field must be a dictionary')
            # add the $ character to the keys in query_params that don't begin with it
            query_params = dict(('$%s' % k if not k.startswith('$') else k, v)
                                for k, v in query_params.iteritems())
        parser = Parser()
        query_model = parser.parse(query)
        drf = self._get_drivers_factory(self.ehr_repository)
        with drf.get_driver() as driver:
            # the count_only field will be retrieved parsing AQL query
            results_set = driver.execute_query(query_model, self.patients_repository, self.ehr_repository,
                                               query_params, count_only, query_processes)
        return results_set
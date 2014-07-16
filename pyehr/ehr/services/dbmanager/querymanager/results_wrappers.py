class ResultColumnDef(object):

    def __init__(self, alias=None, path=None):
        self.path = path.strip()
        self.alias = alias.strip()

    def __eq__(self, other):
        if isinstance(other, ResultColumnDef):
            return self.get_json() == other.get_json()
        else:
            return False

    def get_json(self):
        return {'alias': self.alias, 'path': self.path}


class ResultRow(object):

    def __init__(self, record):
        self.record = record

    def __eq__(self, other):
        if isinstance(other, ResultRow):
            return self.record == other.record
        else:
            return False


class ResultSet(object):

    def __init__(self):
        self.name = None
        self.total_results = 0
        self.columns = []
        self.rows = []

    def get_json(self):
        columns = [col.get_json() for col in self.columns]
        rows = [[i for i in r.record] for r in self.rows]
        json = {
            "name": self.name,
            "totalResults": self.total_results,
            "columns": columns,
            "rows": rows
        }
        return json

    def _get_alias(self, key):
        for col in self.columns:
            if col.path == key:
                return col.alias
        raise KeyError('Can\'t map key %s' % key)

    def __str__(self):
        return str(self.get_json())

    def extend(self, result_set):
        self.total_results += result_set.total_results
        self.rows += result_set.rows
        for c in result_set.columns:
            if c not in self.columns:
                self.columns.append(c)

    def add_column_definition(self, colum_def):
        if not colum_def in self.columns:
            self.columns.append(colum_def)

    def add_row(self, row):
        self.rows.append(row)
        self.total_results += 1

    @property
    def results(self):
        for r in self.rows:
            yield {self._get_alias(k): v for k, v in r.record.iteritems()}
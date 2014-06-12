class ResultColumnDef(object):
    def __init__(self):
        self.path = None
        self.name = None


class ResultRow(object):
    def __init__(self):
        self.items = []


class ResultSet(object):
    def __init__(self):
        self.name = None
        self.total_results = 0
        self.columns = []
        self.rows = []

    def get_json(self):
        columns = []
        for col in self.columns:
            columns.append(
                {
                    'name': col.name,
                    'path': col.path
                }
            )
        rows = []
        for r in self.rows:
            row = []
            for i in r.items:
                row.append(i)
            rows.append(row)
        json = {
            "name": self.name,
            "totalResults": self.total_results,
            "columns": columns,
            "rows": rows
        }
        return json

    def __str__(self):
        return str(self.get_json())
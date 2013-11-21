__author__ = 'ciccio'

from openehr.aql.parser import Parser as Parser

class ResultColumnDef():
    def __init__(self):
        self.path = None
        self.name = None

class ResultRow():
    def __init__(self):
        self.items = []

class ResultSet():
    def __init__(self):
        self.name = None
        self.totalResults = 0
        self.columns = []
        self.rows = []

    def getJSON(self):
        columns = []
        for col in self.columns:
            columns.append({'name' : col.name, 'path' : col.path})
        rows = []
        for r in self.rows:
            row = []
            for i in r.items:
                row.append(i)
            rows.append(row)
        json = {"name" : self.name,
            "totalResults" : self.totalResults,
            "columns" : columns,
            "rows" : rows
        }
        return json

    def __str__(self):
        return str(self.getJSON())
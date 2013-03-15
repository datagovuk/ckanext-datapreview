"""Data Proxy - Messytables transformation adapter"""
import urllib2
import xlrd
import base
from ckanext.datapreview.lib.errors import ResourceError
from messytables import AnyTableSet

try:
    import json
except ImportError:
    import simplejson as json

class TabularTransformer(base.Transformer):

    def __init__(self, resource, url, query):
        super(TabularTransformer, self).__init__(resource, url, query)
        self.requires_size_limit = True

        if 'worksheet' in self.query:
            self.sheet_number = int(self.query.getfirst('worksheet'))
        else:
            self.sheet_number = 0

        self.type = query.get('type')


    def transform(self):
        handle = self.open_data(self.url)

        if not handle:
            raise ResourceError("Remote resource missing",
                "Unable to load the remote resource")

        table_set = AnyTableSet.from_fileobj(handle, extension=self.type)
        tables = table_set.tables

        tp = 0
        while tp < len(tables):
            # Find a workable sheet with more than 0 rows
            rowset = list(tables[tp])
            if len(rowset) > 0:
                break
            tp += 1

        result = {
            "fields": [],
            "data": [[unicode(c.value) for c in r] for r in rowset[:self.max_results]],
            "max_results": self.max_results,
        }
        if len(rowset):
            result["fields"] = [unicode(c.value) for c in rowset.pop(0)]

        self.close_stream(handle)

        return result


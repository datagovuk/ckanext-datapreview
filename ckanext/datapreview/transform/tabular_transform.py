"""Data Proxy - Messytables transformation adapter"""
import base
from ckanext.datapreview.lib.errors import ResourceError
from messytables import AnyTableSet


class TabularTransformer(base.Transformer):

    def __init__(self, resource, url, query, mimetype=None):
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

        table_set = AnyTableSet.from_fileobj(handle, extension=self.type, mimetype=self.mimetype)
        tables = table_set.tables

        tp = 0
        rs = []
        while tp < len(tables):
            # Find a workable sheet with more than 0 rows
            rs = list(tables[tp])
            if len(rs) > 0:
                break
            tp += 1

        result = {
            "fields": [],
            "data": [[unicode(c.value) for c in r] for r in rs[:self.max_results]],
            "max_results": self.max_results,
        }
        if len(rs):
            result["fields"] = [unicode(c.value) for c in rs.pop(0)]

        self.close_stream(handle)

        return result

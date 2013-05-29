"""Data Proxy - Messytables transformation adapter"""
import base
from ckanext.datapreview.lib.errors import ResourceError
from messytables import any_tableset


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

        table_set = any_tableset(fileobj=handle,
                                 extension=self.type,
                                 mimetype=self.mimetype)
        tables = table_set.tables

        # Find a workable sheet with more than 0 rows
        rows = []
        for table in tables:
            # + 1 so that the header is included
            rows = _list(table, self.max_results + 1)
            if len(rows) > 0:
                break

        # top row becomes 'fields'. Convert to unicode.
        fields = [unicode(c.value) for c in rows.pop(0)] if rows else []
        # other rows become 'data'. Convert to unicode.
        data = [[unicode(c.value) for c in r] for r in rows[:self.max_results]]
        result = {
            "fields": fields,
            "data": data,
            "max_results": self.max_results,
        }

        self.close_stream(handle)

        return result

def _list(iterable, max_results):
    '''Returns the list(iterable) up to a maximum number of results'''
    out = []
    count = 0
    for item in iterable:
        out.append(item)
        count += 1
        if count == max_results:
            break
    return out

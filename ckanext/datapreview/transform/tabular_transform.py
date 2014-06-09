"""Data Proxy - Messytables transformation adapter"""
import base
from ckanext.datapreview.lib.errors import ResourceError
from messytables import any_tableset, headers_guess


class TabularTransformer(base.Transformer):

    def __init__(self, resource, url, query, mimetype=None):
        super(TabularTransformer, self).__init__(resource, url, query)
        self.requires_size_limit = True

        if 'worksheet' in self.query:
            self.sheet_number = int(self.query.getfirst('worksheet'))
        else:
            self.sheet_number = 0

        self.type = query.get('type')
        self.from_archive = query.get('archived', False)

    def transform(self):
        handle = self.open_data(self.url)

        if not handle:
            raise ResourceError("Remote resource missing",
                "Unable to load the remote resource")

        try:
            table_set = any_tableset(fileobj=handle,
                                     extension=self.type,
                                     mimetype=self.mimetype)
        except Exception, e:
            raise ResourceError("Resource loading error",
                "Unable to load the resource")

        tables = table_set.tables

        # Find a workable sheet with more than 0 rows
        rows = []
        row_count = 0
        for table in tables:
            # + 1 so that the header is included
            row_count = len(list(table))
            rows = _list(table, self.max_results + 1)
            if len(rows) > 0:
                break

        # Use the built-in header guessing in messtables to find the fields
        offset, headers = headers_guess(rows)
        fields =  [unicode(c) for c in headers] if headers else []

        # other rows become 'data'. Convert to unicode.
        # We should skip row offset so that we don't re-display the headers
        data = []
        for i, r in enumerate(rows[:self.max_results]):
            if i != offset:
                data.append([unicode(c.value) for c in r])

        extra = ""

        if len(tables) > 1:
            extra = "Only 1 of {0} tables shown".format(len(tables))
            if row_count > self.max_results:
                extra = extra + " and {0} rows from {1} in this table".format(self.max_results, row_count)
        else:
            if row_count > self.max_results:
                extra = "This preview shows only {0} rows from {1}".format(self.max_results, row_count)

        result = {
            "fields": fields,
            "data": data,
            "max_results": self.max_results,
            "extra_text": extra,
            "archived": "This file is previewed from the data.gov.uk archive." if self.from_archive else ""
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

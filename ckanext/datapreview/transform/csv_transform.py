"""Data Proxy - CSV transformation adapter"""
import urllib2
import csv
from ckanext.datapreview.transform.base import Transformer
import brewery.ds as ds


try:
    import json
except ImportError:
    import simplejson as json

class CSVTransformer(Transformer):
    def __init__(self, resource, url, query):
        super(CSVTransformer, self).__init__(resource, url, query)
        self.requires_size_limit = False

        self.encoding = self.query.get("encoding", 'utf-8')
        self.dialect = self.query.get("dialect")

    def _might_be_html(self, content):
        count = content.count('<')

        if count >= 3:
            if content.count('>') > 1:
                return dict(title="Invalid content",
                    message="This content appears to be HTML and not tabular data")
        return None


    def transform(self):
        handle = self.open_data(self.url)

        if not self.dialect:
            if self.url.endswith('.tsv'):
                self.dialect = 'excel-tab'
            else:
                self.dialect = 'excel'

        src = ds.CSVDataSource(handle, encoding=self.encoding, dialect=self.dialect)
        src.initialize()

        try:
            result = self.read_source_rows(src)
        except:
            # We so often get HTML when someone tells us it is CSV
            # that we will have this extra special check JUST for this
            # use-case.
            self.close_stream(handle)

            check = self._might_be_html(self.open_data(self.url).read())
            if check:
                return check
            raise

        self.close_stream(handle)

        return result


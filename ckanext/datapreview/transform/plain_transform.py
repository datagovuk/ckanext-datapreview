"""Data Proxy - Plain transformation adapter"""
import urllib2
from ckanext.datapreview.transform.base import Transformer

try:
    import json
except ImportError:
    import simplejson as json

class PlainTransformer(Transformer):
    """
    A plain transformer that just packages the data up (assuming it is within
    the size limit). Recline is expecting a list of lists (rows of cells) and
    so the data is wrapped up in that format ready for display.
    """

    def __init__(self, resource, url, query):
        super(PlainTransformer, self).__init__(resource, url, query)
        self.requires_size_limit = True

    def transform(self):
        handle = self.open_data(self.url)

        result = {
                    "fields": ["data"],
                    "data": [["%s" % (handle.read())]]
                  }
        handle.close()

        return result


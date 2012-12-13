"""Data Proxy - CSV transformation adapter"""
import urllib2
from ckanext.datapreview.transform.base import Transformer

try:
    import json
except ImportError:
    import simplejson as json

class PlainTransformer(Transformer):
    def __init__(self, resource, url, query):
        super(PlainTransformer, self).__init__(resource, url, query)
        self.requires_size_limit = True

    def transform(self):
        handle = urllib2.urlopen(self.url)

        result = {
                    "fields": ["data"],
                    "data": [["%s" % (handle.read())]]
                  }
        handle.close()

        return result


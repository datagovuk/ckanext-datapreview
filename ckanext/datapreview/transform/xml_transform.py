"""Data Proxy - CSV transformation adapter"""
import urllib2
import xml.dom.minidom
from ckanext.datapreview.transform.base import Transformer
import brewery.ds as ds

try:
    import json
except ImportError:
    import simplejson as json

class XMLTransformer(Transformer):
    """
    Transforms XML based formats (atom, xml, rss, rdf etc) and pretty prints
    the output so that it is delivered in a readable format.
    """

    def __init__(self, resource, url, query):
        super(XMLTransformer, self).__init__(resource, url, query)
        self.requires_size_limit = True

    def transform(self):
        handle = self.open_data(self.url)

        dom = xml.dom.minidom.parseString(handle.read())
        pretty = dom.toprettyxml(indent='   ')
        result = {
                    "fields": ["data"],
                    "data": [["%s" % (pretty)]]
                  }
        handle.close()

        return result

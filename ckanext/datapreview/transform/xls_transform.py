"""Data Proxy - XLS transformation adapter"""
import urllib2
import xlrd
import base
import brewery.ds as ds

try:
    import json
except ImportError:
    import simplejson as json

class XLSTransformer(base.Transformer):
    def __init__(self, resource, url, query):
        super(XLSTransformer, self).__init__(resource, url, query)

        if 'worksheet' in self.query:
            self.sheet_number = int(self.query.getfirst('worksheet'))
        else:
            self.sheet_number = 0

    def transform(self):
        handle = self.open_data(self.url)

        if not handle:
            return dict(title="Remote resource missing",
                message="Unable to load the remote resource")

        src = ds.XLSDataSource(handle, sheet = self.sheet_number)

        try:
            src.initialize()
            result = self.read_source_rows(src)
        except ValueError:
            return dict(title="Invalid content",
                message="Unable to process the XLS file")
        except Exception as e:
            # Read the 100 bytes, and strip it. If first
            # char is < then it is HTML. Sigh.
            self.close_stream(handle)
            data = self.open_data(self.url).read(100)
            if not data.strip():
                return dict(title="Invalid content",
                    message="This resource does not appear to be an XLS file")

            if data.strip()[0] == '<':
                return dict(title="Invalid content",
                    message="This content appears to be HTML and not tabular data")
            raise


        self.close_stream(handle)

        return result


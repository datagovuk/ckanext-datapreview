"""Data Proxy - Plain transformation adapter"""
from ckanext.datapreview.transform.base import Transformer
from ckanext.datapreview.lib.errors import ResourceError

MAX_TEXT_SIZE = 8192


class PlainTransformer(Transformer):
    """
    A plain transformer that just packages the data up (assuming it is within
    the size limit). Recline is expecting a list of lists (rows of cells) and
    so the data is wrapped up in that format ready for display.
    """

    def __init__(self, resource, url, query):
        self.size = int(query.get('length', 0))
        self.from_archive = query.get('archived', False)
        super(PlainTransformer, self).__init__(resource, url, query)

    def transform(self):
        handle = self.open_data(self.url)
        if not handle:
            raise ResourceError("Remote resource missing",
                                "Unable to load the remote resource")

        data = handle.read(MAX_TEXT_SIZE)
        extra_text = ''
        if self.size and (self.size > MAX_TEXT_SIZE):
            data += "\n... [output truncated]"
            extra_text = 'Only the first %sk shown - download it for the full'\
                         ' file.' % int(MAX_TEXT_SIZE/1024.0)

        data = data.decode('utf-8', 'ignore')
        result = {
                    "fields": ["data"],
                    "data": [["%s" % (data)]],
                    "extra_text": extra_text,
                    "archived": "This file is previewed from the data.gov.uk archive." if self.from_archive else ""
                  }

        self.close_stream(handle)

        return result

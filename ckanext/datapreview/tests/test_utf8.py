
from ckan.model.resource import Resource
from nose.tools import assert_equal
from ckanext.datapreview.controller import DataPreviewController
import os, glob

class TestUTF8:

    @classmethod
    def setup_class(cls):
        pass

    @classmethod
    def teardown_class(cls):
        pass

    def test_get_url(self):
        # Build a fake resource and a fake DataPreviewController
        # and make sure that _get_url correctly decodes the UTF-8
        r = Resource(url="")
        p = os.path.join(os.path.dirname(__file__), "data/utf8/*.xls")
        p = glob.glob(p)

        r.extras['cache_filepath'] = p[0].decode('utf8')
        ctr = DataPreviewController()
        s = ctr._get_url(r, {})

        assert s == r.extras['cache_filepath']


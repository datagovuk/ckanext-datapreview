import collections
import logging
import datetime
import os
import re
import time
import sys
import requests
import urlparse

from pylons import config
from ckan.lib.cli import CkanCommand
# No other CKAN imports allowed until _load_config is run,
# or logging is disabled

class PrepResourceCache(CkanCommand):
    """"""
    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 1
    min_args = 1

    def __init__(self, name):
        super(PrepResourceCache, self).__init__(name)


    def command(self):
        """ Helpful command for development """
        from ckan.logic import get_action
        import urlparse, operator

        self._load_config()
        log = logging.getLogger(__name__)

        import ckan.model as model
        model.Session.remove()
        model.Session.configure(bind=model.meta.engine)
        model.repo.new_revision()

#        if not config.get('debug',True) or os.environ.get('DP_OVERRIDE'):
#            print 'Do not run this on a production DB'
#            return

        r = model.Resource.get(self.args[0])
        if not r:
            print 'Cannot find resource - ' + self.args[0]
            return

        if r.cache_url:
            print 'Already has a cache_url ' + r.cache_url

        folder = os.path.join("/tmp/%s" % (self.args[0][0:2],), self.args[0])
        if not os.path.exists(folder):
            os.makedirs(folder)

        print "Fetching " + r.url
        req = requests.get(r.url)
        filename = os.path.basename(urlparse.urlparse(r.url).path)
        p = os.path.join(folder, filename)
        with open(p, 'w+b') as f:
            f.write(req.content)

        root = "%s%s/%s/%s" % (config.get('ckan.cache_url_root'),self.args[0][0:2],self.args[0],filename)
        print root
        r.cache_url = root
        model.Session.add(r)
        model.Session.commit()


#ckan.cache_url_root = http://localhost:5000/data/resource_cache/


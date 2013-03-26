
import logging
import os
import time
import json
import requests
import urlparse

from pylons import config
from ckan.lib.cli import CkanCommand
# No other CKAN imports allowed until _load_config is run,
# or logging is disabled

log = logging.getLogger(__file__)


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

        self._load_config()
        log = logging.getLogger(__name__)

        import ckan.model as model
        model.Session.remove()
        model.Session.configure(bind=model.meta.engine)
        model.repo.new_revision()

        if not config.get('debug', True) or os.environ.get('DP_OVERRIDE'):
            print 'Do not run this on a production DB'
            return

        r = model.Resource.get(self.args[0])
        if not r:
            log.info('Cannot find resource - ' + self.args[0])
            return

        if r.cache_url:
            log.info('Already has a cache_url ' + r.cache_url)

        folder = os.path.join("/tmp/%s" % (self.args[0][0:2],), self.args[0])
        if not os.path.exists(folder):
            os.makedirs(folder)

        log.info("Fetching " + r.url)
        req = requests.get(r.url)
        filename = os.path.basename(urlparse.urlparse(r.url).path)
        p = os.path.join(folder, filename)
        with open(p, 'w+b') as f:
            f.write(req.content)

        root = "%s%s/%s/%s" % (config.get('ckan.cache_url_root'),
            self.args[0][0:2], self.args[0], filename)
        r.cache_url = root.replace(' ', '%20')
        model.Session.add(r)
        model.Session.commit()


class StrawPollPreviewTest(CkanCommand):
    """"""
    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 1
    min_args = 0

    def __init__(self, name):
        super(StrawPollPreviewTest, self).__init__(name)
        self.parser.add_option('-t', '--test',
                               action='store_true',
                               default=False,
                               dest='test',
                               help='Whether to compare to the jsondataproxy')
        self.parser.add_option('-m', '--many',
                               type='int',
                               default=20,
                               dest='count',
                               help='Process n records')

    def command(self):
        """ Helpful command for development """
        from sqlalchemy import func

        self._load_config()
        self.log = logging.getLogger(__name__)

        import ckan.model as model
        model.Session.remove()
        model.Session.configure(bind=model.meta.engine)
        model.repo.new_revision()

        formats = ['csv', 'xls']
        if len(self.args) == 1:
            formats = self.args[0].split(',')

        log.info("Processing %s" % ' and '.join(formats))
        for fmt in formats:
            q = model.Session.query(model.Resource)\
                .filter(func.lower(model.Resource.format) == func.lower(fmt))\
                .filter(model.Resource.state == 'active')

            total = q.count()
            records = q.order_by(func.random()).limit(self.options.count).all()

            self.log.info("We have %d records from %d files of %s format" %
                          (len(records), total, fmt))
            self.log.info("=" * 50)

            success_count, fail_count = 0, 0
            for r in records:
                t0 = time.time()
                success, msg = self._test_resource(r)
                duration = time.time() - t0

                if success:
                    self.log.info("  OK (%0.2fs) - %s" % (duration, r.id))
                    success_count = success_count + 1
                else:
                    self.log.info("  Fail (%0.2fs)- %s - %s" %
                                  (duration, r.id, msg))
                    fail_count = fail_count + 1

                if self.options.test:
                    self._test_jsondataproxy(r)

            print "Out of %d records processed there were %d successes and %d failures" % (len(records),success_count,fail_count)

    def _test_jsondataproxy(self, resource):
        t0 = time.time()
        url = urlparse.urljoin('http://jsonpdataproxy.appspot.com/',
                               '?url=%s&format=json' % (resource.url,))
        req = requests.get(url)
        duration = time.time() - t0

        if not req.status_code == 200:
            self.log.info("  Fail (%0.2fs) - JSON Data Proxy - %d on %s\n" %
                          (duration, req.status_code, resource.url))
            return

        data = json.loads(req.content)
        if 'error' in data:
            self.log.info("  OK (%0.2fs) - JSON Data Proxy -  %s\n" %
                          (duration, data['error']['title']))
            return

        self.log.info("  OK (%0.2fs) - JSON Data Proxy\n" % duration)

    def _test_resource(self, resource):

        host = config['ckan.site_url']
        url = urlparse.urljoin(host, '/data/preview/%s' % (resource.id,))
        req = requests.get(url)
        if not req.status_code == 200:
            return False, "Server returned %d" % req.status_code

        data = json.loads(req.content)
        if 'error' in data:
            return False, "Received error %s" % data

        return True, ""

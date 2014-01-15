import os
import logging
import json
import urllib2
from pylons import config
import ckan.model as model
from ckan.lib.base import (BaseController, c, request, response, abort)
from ckanext.dgu.plugins_toolkit import NotAuthorized
from ckan.logic import check_access

log = logging.getLogger('ckanext.datapreview')

from ckanext.datapreview.lib.helpers import (proxy_query,
                                             get_resource_format_from_qa,
                                             identify_resource,
                                             fix_url)
from ckanext.datapreview.lib.errors import ProxyError


def _error(**vars):
    return json.dumps(dict(error=vars), indent=4)

class DataPreviewController(BaseController):

    def index(self, id):
        resource = model.Resource.get(id)
        if not resource or resource.state != 'active':
            abort(404, "Resource not found")

        context = {'model': model,
                   'session': model.Session,
                   'user': c.user}
        try:
            check_access("resource_show", context, {'id': resource.id})
        except NotAuthorized, e:
            abort(403, "You are not permitted access to this resource")

        size_limit = config.get('ckan.datapreview.limit', 5000000)

        format_ = get_resource_format_from_qa(resource)
        if format_:
            log.debug("QA thinks this file is %s" % format_)
        else:
            log.debug("Did not find QA's data format")
            format_ = resource.format.lower() if resource.format else ''

        query = dict(type=format_, size_limit=size_limit, length=None)
        if resource.size:
            query['length'] = resource.size

        # Add the extra fields if they are set
        for k in ['max-results', 'encoding', 'type']:
            if k in request.params:
                query[k] = request.params[k]

        url, archived = self._get_url(resource, query)
        query['archived'] = archived
        if url:
            try:
                response.content_type = 'application/json'
                result = proxy_query(resource, url, query)
            except ProxyError as e:
                log.warn("Request {0} failed : {1}".format(identify_resource(resource), e))
                result = _error(title=e.title, message=e.message)
        else:
            result = _error(title="Remote resource not downloadable",
                message="Unable to find the remote resource for download")

        format_ = request.params.get('callback')
        if format_:
            return "%s(%s)" % (format_, result)

        return result

    def _get_url(self, resource, query):
        '''
        Given a resource, return the URL for the data and a flag denoting whether
        the URL is to a local file (and therefore can ignore size limit checks.)

        This allows a local cache to be used in preference to the
        resource.url.

        If we are going to use an external URL, then we can do a HEAD request
        to check it works and record the mimetype & length in the query dict.

        :param resource: resource object
        :param query: dict describing the properties of the data
        '''
        from requests.exceptions import InvalidURL

        url = None
        archived = False
        query['mimetype'] = None

        # Look for a local cache of the data file
        # e.g. "cache_filepath": "/mnt/shared/ckan_resource_cache/63/63b159d7-90c5-443b-846d-f700f74ea062/bian-anal-mca-2005-dols-eng-1011-0312-tab2.csv"
        cache_filepath = resource.extras.get('cache_filepath')
        if cache_filepath:
            if os.path.exists(cache_filepath.encode('utf8')):
                log.debug('Previewing local cached data: %s', cache_filepath)
                url = cache_filepath
                archived = True
            else:
                log.debug('Local cached data file missing: %s', cache_filepath)

        # Otherwise try the cache_url
        # This works well when running on a database copied from another
        # machine - all the cached files are missing locally, but it can use
        # them from the original machine using the cache_url.
        if not url and hasattr(resource, 'cache_url') and resource.cache_url:
            try:
                u = fix_url(resource.cache_url)
            except InvalidURL:
                log.error("Unable to fix the URL for resource: %s" % resource.id)
                return None, False

            # e.g. resource.cache_url = "http://data.gov.uk/data/resource_cache/07/0791d492-8ab9-4aae-b7e6-7ecae561faa3/bian-anal-mca-2005-dols-eng-1011-0312-qual.pdf"
            try:
                req = urllib2.Request(u)
                req.get_method = lambda: 'HEAD'

                r = urllib2.urlopen(req)
                if r.getcode() == 200:
                    url = u
                    query['length'] = r.info().get("content-length", 0)
                    query['mimetype'] = r.info().get('content-type', None)
                    log.debug('Previewing cache URL: %s', url)
            except Exception, e:
                log.error(u"Request {0} with cache url {1}, {2}".format(identify_resource(resource), u, e))

        # Otherwise use the URL itself
        if not url:
            try:
                u = fix_url(resource.url)
            except InvalidURL:
                log.error("Unable to fix the URL for resource: %s" % resource.id)
                return None, False

            try:
                req = urllib2.Request(u)
                req.get_method = lambda: 'HEAD'

                r = urllib2.urlopen(req)
                if r.getcode() == 200:
                    url = u
                    query['length'] = r.info().get("content-length", 0)
                    query['mimetype'] = r.info().get('content-type', None)
                    log.debug('Previewing direct from URL: %s', url)
                elif r.getcode() > 400:
                    return None
            except urllib2.HTTPError, err:
                if err.code < 400:
                    log.warn(u"Request {0} with url {1}, {2}".format(identify_resource(resource), u, err.code))
            except Exception, e:
                log.error(u"Request {0} with url {1}, {2}".format(identify_resource(resource), u, e))

        return url, archived

    def serve(self, path):
        root = os.path.join(config.get('ckanext-archiver.archive_dir', '/tmp'),
                            path).replace(' ', '%20')

        if not os.path.exists(root):
            abort(404)
        response.content_type = 'application/json'
        return str(open(root).read())

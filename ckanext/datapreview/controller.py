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

from ckanext.datapreview.lib.helpers import proxy_query
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

        # We will use the resource specified format to determine what
        # type of file this is.  At some point QA will be providing a much
        # more accurate idea of the file-type which will mean we can avoid
        # those cases where HTML is marked as XLS, or XSL as CSV.
        typ = request.params.get('type',
                                 resource.format.lower()
                                 if resource.format else '')

        query = dict(type=typ, size_limit=size_limit)

        # Add the extra fields if they are set
        for k in ['max-results', 'encoding']:
            if k in request.params:
                query[k] = request.params[k]

        url = self._get_url(resource, query)

        try:
            response.content_type = 'application/json'
            result = proxy_query(resource, url, query)
        except ProxyError as e:
            log.error("Request id {0}, {1}".format(id, e))
            result = _error(title=e.title, message=e.message)

        fmt = request.params.get('callback')
        if fmt:
            return "%s(%s)" % (fmt, result)

        return result

    def _get_url(self, resource, query):
        # To determine where we will get the resource data from, we will try
        # in order:
        # 1. If there is a cache_url, guess where the local file might be
        # 2. cache_url, if set. If we get a 404 from the head request then ..
        # 3. resource url.

        url = None

        if hasattr(resource, 'cache_url') and resource.cache_url:
            dir_root = config.get('ckanext-archiver.archive_dir')
            url_root = config.get('ckan.cache_url_root')
            if dir_root and url_root:
                possible = os.path.join(dir_root,
                    resource.cache_url[len(url_root):]).encode('latin-1')
                if os.path.exists(possible):
                    url = possible
                    log.debug("Using local_file at %s" % url)

            if not url:  # If not found on disk try a head request
                try:
                    req = urllib2.Request(resource.cache_url)
                    req.get_method = lambda: 'HEAD'

                    r = urllib2.urlopen(req)
                    if r.getcode() == 200:
                        url = resource.cache_url
                        query['length'] = r.info()["content-length"]
                except Exception, e:
                    log.error("Request {0}, with url {1}, {2}".format(id, resource.cache_url, e))

        if not url:
            url = resource.url

        return url

    def serve(self, path):
        root = os.path.join(config.get('ckanext-archiver.archive_dir', '/tmp'),
                            path).replace(' ', '%20')

        if not os.path.exists(root):
            abort(404)
        response.content_type = 'application/json'
        return str(open(root).read())

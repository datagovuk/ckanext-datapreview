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

from ckanext.datapreview.lib.helpers import proxy_query, get_resource_format_from_qa
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

        fmt = get_resource_format_from_qa(resource)
        if fmt:
            log.debug("QA thinks this file is %s" % fmt)
        else:
            log.debug("Did not find QA's data format")
            fmt = resource.format.lower() if resource.format else ''

        query = dict(type=fmt, size_limit=size_limit)

        # Add the extra fields if they are set
        for k in ['max-results', 'encoding']:
            if k in request.params:
                query[k] = request.params[k]

        url = self._get_url(resource, query)
        if url:
            try:
                response.content_type = 'application/json'
                result = proxy_query(resource, url, query)
            except ProxyError as e:
                log.error("Request id {0}, {1}".format(resource.id, e))
                result = _error(title=e.title, message=e.message)
        else:
            result = _error(title="Remote resource not downloadable",
                message="Unable to find the remote resource for download")

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
        query['mimetype'] = None

        if hasattr(resource, 'cache_url') and resource.cache_url:
            dir_root = config.get('ckanext-archiver.archive_dir')
            url_root = config.get('ckan.cache_url_root')
            if dir_root and url_root:
                possible = os.path.join(dir_root,
                    resource.cache_url[len(url_root):]).encode('utf-8')
                if os.path.exists(possible):
                    url = possible
                    log.debug("Using local_file at %s" % url)

            if not url:  # If not found on disk try a head request
                try:
                    req = urllib2.Request(resource.cache_url.encode('utf8'))
                    req.get_method = lambda: 'HEAD'

                    r = urllib2.urlopen(req)
                    if r.getcode() == 200:
                        url = resource.cache_url
                        query['length'] = r.info()["content-length"]
                        query['mimetype'] = r.info().get('content-type', None)
                except Exception, e:
                    log.error(u"Request {0}, with url {1}, {2}".format(resource.id, resource.cache_url, e))

        if not url:
            try:
                req = urllib2.Request(resource.url.encode('utf8'))
                req.get_method = lambda: 'HEAD'

                r = urllib2.urlopen(req)
                if r.getcode() == 200:
                    url = resource.url
                    query['length'] = r.info()["content-length"]
                    query['mimetype'] = r.info().get('content-type', None)
                elif r.getcode() > 400:
                    return None

            except Exception, e:
                log.error(u"Request {0}, with url {1}, {2}".format(resource.id, resource.url, e))

        return url

    def serve(self, path):
        root = os.path.join(config.get('ckanext-archiver.archive_dir', '/tmp'),
                            path).replace(' ', '%20')

        if not os.path.exists(root):
            abort(404)
        response.content_type = 'application/json'
        return str(open(root).read())

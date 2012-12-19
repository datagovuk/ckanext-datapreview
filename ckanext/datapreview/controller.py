import os
import logging
import json
import requests
from pylons import config
import ckan.model as model
from ckan.lib.base import (BaseController, c, request, response, abort)
from ckanext.dgu.plugins_toolkit import NotAuthorized
from ckan.logic import check_access

log = logging.getLogger('ckanext.datapreview')

from ckanext.datapreview.lib.helpers import proxy_query, ProxyError


def _error(**vars):
    return json.dumps(dict(error=vars), indent=4)


class DataPreviewController(BaseController):

    def index(self, id):
        resource = model.Resource.get(id)
        if not resource or resource.state != 'active':
            abort(404, "Resource not found")

        context = {'model': model,
                   'session':model.Session,
                   'user': c.user}
        try:
            check_access("resource_show", context, {'id': resource.id})
        except NotAuthorized, e:
            abort(403, "You are not permitted access to this resource")

        size_limit = config.get('ckan.datapreview.limit', 1000000)

        # We will use the resource specified format to determine what
        # type of file this is.  We could extend this to use the file
        # detector in cases where this is not set.
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
            log.error(e)
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
                    resource.cache_url[len(url_root):])
                if os.path.exists(possible):
                    url = possible

            if not url:  # If not found on disk
                r = requests.head(resource.cache_url)
                if r.status_code == 200:
                    url = resource.cache_url
                    query['length'] = r.headers['content-length']

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

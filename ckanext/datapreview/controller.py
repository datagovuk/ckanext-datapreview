import os
import sys
import logging
import operator
import collections
from ckan.lib.base import (BaseController, c, g, render, request, response, abort)
from pylons import config
import sqlalchemy
from sqlalchemy import func, cast, Integer
import ckan.model as model

log = logging.getLogger('ckanext.datapreview')


from ckanext.datapreview.lib import AttributeDict
from ckanext.datapreview.lib.helpers import proxy_query, ProxyError


class DataPreviewController(BaseController):

    def index(self, id):
        resource = model.Resource.get(id)
        if not resource or resource.state != 'active':
            abort(404, "Resource not found")

        typ = request.params.get('type',
                                 resource.format.lower()
                                 if resource.format else '')

        query = {'type': typ}
        for k in ['max-results', 'encoding']:
            if k in request.params:
                query[k] = request.params[k]

        try:
            result = proxy_query(resource, resource.url, query)

            response.content_type = 'application/json'
        except ProxyError as e:
            log.error(e)
            result = str(e)

        return result


    def serve(self, path):
        root = os.path.join(config.get('ckanext-archiver.archive_dir', '/tmp'),
                            path)
        if not os.path.exists(root):
            abort(404)
        response.content_type = 'application/json'
        return str(open(root).read())
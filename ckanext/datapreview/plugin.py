import logging
from pylons import config
import ckan.lib.helpers as h
import ckan.plugins as p
from ckan.plugins import implements, toolkit
from ckan.logic import get_action


log = logging.getLogger('ckanext.datapreview')


class DataPreviewPlugin(p.SingletonPlugin):
    implements(p.IConfigurer, inherit=True)
    implements(p.IRoutes, inherit=True)

    def update_config(self, config):
        toolkit.add_template_directory(config, 'templates')
        toolkit.add_public_directory(config, 'public')

    def after_map(self, map):
        map.connect(
            '/data/preview/{id}',
            controller='ckanext.datapreview.controller:DataPreviewController',
            action='index'
        )
        if config.get('debug', False):
            map.connect(
                '/data/resource_cache/{path:.*}',
                controller='ckanext.datapreview.controller:DataPreviewController',
                action='serve'
            )
        return map


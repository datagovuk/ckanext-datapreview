import logging
import sys
from pylons import config
import ckan.plugins as p
from ckan.plugins import implements, toolkit

log = logging.getLogger('ckanext.datapreview')


class DataPreviewPlugin(p.SingletonPlugin):
    implements(p.IConfigurer, inherit=True)
    implements(p.IRoutes, inherit=True)

    def update_config(self, config):
        # Work around an issue with XLRD 0.8 where it logs to stdout by
        # accident (and wsgi tells it off).
        sys.stdout = sys.stderr

        toolkit.add_template_directory(config, 'templates')
        toolkit.add_public_directory(config, 'public')

    def after_map(self, map):
        controller = 'ckanext.datapreview.controller:DataPreviewController'
        map.connect(
            '/data/preview/{id}',
            controller=controller,
            action='index'
        )
        if config.get('debug', False):
            map.connect(
                '/data/resource_cache/{path:.*}',
                controller=controller,
                action='serve'
            )
        return map

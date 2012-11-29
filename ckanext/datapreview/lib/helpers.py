import re
import os
import urlparse
import httplib
import logging
import json
from ckanext.datapreview.lib import AttributeDict
from ckanext.datapreview.transform.base import transformer

log = logging.getLogger('ckanext.datapreview.lib.helpers')

def get_resource_length(url, required = False, redirects = 0):
    """Get length of a resource"""

    parts = urlparse.urlparse(url)

    connection = httplib.HTTPConnection(parts.netloc)

    try:
        connection.request("HEAD", parts.path)
    except Exception, e:
        raise ResourceError("Unable to access resource", "Unable to access resource. Reason: %s" % e)

    res = connection.getresponse()

    headers = {}
    for header, value in res.getheaders():
        headers[header.lower()] = value

    # Redirect?
    if res.status == 302 and redirects < REDIRECT_LIMIT:
        if "location" not in headers:
            raise ResourceError("Resource moved, but no Location provided by resource server",
                                    'Resource %s moved, but no Location provided by resource server: %s'
                                    % (parts.path, parts.netloc))

        return get_resource_length(headers["location"], required = required, redirects = redirects + 1)


    if 'content-length' in headers:
        length = int(headers['content-length'])
        return length

    if required:
        raise ResourceError("Unable to get content length",
                                'No content-length returned for server: %s path: %s'
                                % (parts.netloc, parts.path))
    return None

def render(**vars):
    return ["<html>\n"
        "<head>"
        "  <title>%(title)s</title>"
        "</head>\n"
        "<body>\n"
        "  <h1>%(title)s</h1>\n"
        "  <p>%(msg)s</p>\n"
        "</body>\n"
        "</html>\n" %vars
    ]

def error(**vars):
    return json.dumps(dict(error=vars), indent=4)

class ProxyError(StandardError):
    def __init__(self, title, message):
        super(ProxyError, self).__init__()
        self.title = title
        self.message = message
        self.error = "Error"

class ResourceError(ProxyError):
    def __init__(self, title, message):
        super(ResourceError, self).__init__(title, message)
        self.error = "Resource Error"

class RequestError(ProxyError):
    def __init__(self, title, message):
        super(RequestError, self).__init__(title, message)
        self.error = "Request Error"

class HTTPResponseMarble(object):
    def __init__(self, *k, **p):
        self.__dict__['status'] = u'200 OK'
        self.__dict__['status_format'] = u'unicode'
        self.__dict__['header_list'] = [dict(name=u'Content-Type', value=u'text/html; charset=utf8')]
        self.__dict__['header_list_format'] = u'unicode'
        self.__dict__['body'] = []
        self.__dict__['body_format'] = u'unicode'

    def __setattr__(self, name, value):
        if name not in self.__dict__:
            raise AttributeError('No such attribute %s'%name)
        self.__dict__[name] = value


def proxy_query(resource, url, query):
    parts = urlparse.urlparse(url)

    # Get resource type - first try to see whether there is type= URL option,
    # if there is not, try to get it from file extension

    if parts.scheme not in ['http', 'https']:
        raise ResourceError('Only HTTP(S) URLs are supported',
                            'Data proxy does not support %s URLs' % parts.scheme)

    resource_type = query.get("type")
    if not resource_type:
        resource_type = os.path.splitext(parts.path)[1]

    if not resource_type:
        raise RequestError('Could not determine the resource type',
                            'If file has no type extension, specify file type in type= option')

    resource_type = re.sub(r'^\.', '', resource_type.lower())
    try:
        trans = transformer(resource_type, resource, url, query)
    except Exception, e:
        return e
        raise RequestError('Resource type not supported',
                            'Transformation of resource of type %s is not supported. Reason: %s'
                              % (resource_type, e))
    length = get_resource_length(url, trans.requires_size_limit)

    log.debug('The file at %s has length %s', url, length)

    max_length = 1000000

    if length and trans.requires_size_limit and length > max_length:
        raise ResourceError('The requested file is too big to download',
                            'Requested resource is %s bytes. Size limit is %s. '
                            % (length, max_length))

    try:
        result = trans.transform()
    except Exception, e:
        log.debug('Transformation of %s failed. %s: %s', url, e.__class__.__name__, e)
        raise ResourceError("Data Transformation Error",
                            "Data transformation failed. %s: %s" % (e.__class__.__name__, e))
    indent=None

    result["url"] = url
    result["length"] = length

    if query.has_key('indent'):
        indent=int(query.getfirst('indent'))

    return json.dumps(result, indent=indent)

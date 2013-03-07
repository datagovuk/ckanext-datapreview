import re
import os
import urlparse
import urllib2
import logging
import json
import requests
from ckanext.datapreview.transform.base import transformer
from ckanext.datapreview.lib.errors import *

log = logging.getLogger('ckanext.datapreview.lib.helpers')

REDIRECT_LIMIT = 3

def get_resource_length(url, required = False, redirects = 0):
    """Get length of a resource either from a head request to the url, or checking the
    size on disk """
    log.debug('Getting resource length of %s' % url)
    if not url.startswith('http'):
        if not os.path.exists(url):
            raise ResourceError("Unable to access resource",
                "The resource was not found in the resource cache")

        return os.path.getsize(url)

    response = None
    try:
        response = requests.head(url)
    except Exception, e:
        log.error("Unable to access resource: %s" % e)
        raise ResourceError("Unable to access resource",
            "There was a problem retrieving the resource: %s" % e)

    headers = {}
    for header, value in response.headers.iteritems():
        headers[header.lower()] = value

    # Redirect?
    if response.status_code == 302 and redirects < REDIRECT_LIMIT:
        if "location" not in headers:
            raise ResourceError("Resource moved, but no Location provided by resource server",
                'Resource %s moved, but no Location provided by resource server'
                % (url))


        # if our redirect location is relative, then we can only assume
        # it is relative to the url we've just requested.
        if not headers['location'].startswith('http'):
            loc = urlparse.urljoin(url, headers['location'])
        else:
            loc = headers['location']

        return get_resource_length(loc, required=required,
            redirects=redirects + 1)


    if 'content-length' in headers:
        length = int(headers['content-length'])
        return length

    if required:
        # Content length not always set with content-disposition so we will
        # just have to take a flyer on it.
        if not 'content-disposition' in headers:
            log.error('No content-length returned for server: %s'
                                    % (url))
            raise ResourceError("Unable to get content length",
                                    'Unable to find the size of the remote resource')
    return None

def error(**vars):
    return json.dumps(dict(error=vars), indent=4)


def int_formatter(value, places=3, seperator=u','):
    value = str(value)
    if len(value) <= places:
        return value

    parts = []
    while value:
        parts.append(value[-places:])
        value = value[:-places]

    parts.reverse()
    return seperator.join(parts)

def _open_file(url):
    return open(url, 'r')

def _open_url(url):
    """ URLs with &pound; in, just so, so wrong. We also
        can't accept gzip,deflate because gzip lib doesn't
        support working with streams (until 3.2) and we don't
        want to hold the entire file in memory """
    r = requests.get(url.encode('utf-8'), prefetch=False,
                     headers={'accept-encoding': 'identity'})
    if r.status_code == 404:
        return None

    return r.raw


def proxy_query(resource, url, query):
    parts = urlparse.urlparse(url)

    # Get resource type - first try to see whether there is type= URL option,
    # if there is not, try to get it from file extension

    if parts.scheme not in ['http', 'https']:
        query['handler'] = _open_file
    else:
        query['handler'] = _open_url

    resource_type = query.get("type")
    if not resource_type:
        resource_type = os.path.splitext(parts.path)[1]

    if not resource_type:
        raise RequestError('Could not determine the resource type',
                            'If file has no type extension, specify file type in type= option')

    resource_type = re.sub(r'^\.', '', resource_type.lower())
    try:
        trans = transformer(resource_type, resource, url, query)
        if not trans:
            raise Exception("No transformer for %s" % resource_type)
    except Exception, e:
        raise RequestError('Resource type not supported',
                            'Transformation of resource of type %s is not supported.'
                              % (resource_type))

    length = query.get('length', get_resource_length(url,
        trans.requires_size_limit))

    log.debug('The file at %s has length %s', url, length)

    max_length = int(query['size_limit'])

    if length and trans.requires_size_limit and int(length) > max_length:
        raise ResourceError('The requested file is too large to download',
                            'Requested resource is %s bytes. '
                            'Size limit is %s bytes. '
                            % (int_formatter(length),
                            int_formatter(max_length)))

    try:
        result = trans.transform()
    except ResourceError:
        raise
    except StopIteration as si:
        # In all likelihood, there was no data to read
        log.debug('Transformation of %s failed. %s', url,si)
        raise ResourceError("Data Transformation Error",
            "There was a problem reading the resource data")
    except Exception, e:
        log.debug('Transformation of %s failed. %s: %s', url,
            e.__class__.__name__, e)
        raise ResourceError("Data Transformation Error",
            "Data transformation failed. %s: %s" % (e.__class__.__name__, e))

    indent = None

    if url.startswith('http'):
        result["url"] = url
    else:
        result["url"] = resource.cache_url or resource.url

    result["length"] = length or 0

    # Check a few cells to see if this is secretly HTML, more than three <
    # in the fields is a random heuristic that may work. Or not.
    count = 0
    for f in result.get('fields', []):
        count += f.count('<')

    if count >= 3:
        if sum([f.count('>') for f in result['fields']]) > 1:
            raise ResourceError("Invalid format",
                "The resource appears to be HTML")


    if 'indent' in query:
        indent = int(query.getfirst('indent'))

    return json.dumps(result, indent=indent)

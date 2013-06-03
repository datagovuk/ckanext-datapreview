import re
import os
import urlparse
import urllib2
import logging
import json
import requests
from ckanext.datapreview.transform.base import transformer
from ckanext.datapreview.lib.errors import (ResourceError, RequestError)

log = logging.getLogger('ckanext.datapreview.lib.helpers')

REDIRECT_LIMIT = 3

def get_resource_format_from_qa(resource):
    '''Returns the format of the resource, as detected by QA.
    If there is none recorded for this resource, returns None
    '''
    import ckan.model as model
    task_status = model.Session.query(model.TaskStatus).\
                  filter(model.TaskStatus.task_type=='qa').\
                  filter(model.TaskStatus.key=='status').\
                  filter(model.TaskStatus.entity_id==resource.id).first()
    if not task_status:
        return None

    try:
        status = json.loads(task_status.error)
    except ValueError:
        return {}
    return status['format']

def get_resource_length(url, required=False, redirects=0):
    '''Get file size of a resource.

    Either do a HEAD request to the url, or checking the
    size on disk.
    '''
    log.debug('Getting resource length of %s' % url)
    if not url.startswith('http'):
        try:
            if not os.path.exists(url):
                raise ResourceError("Unable to access resource",
                    "The resource was not found in the resource cache")
        except:
            # If the URL is neither http:// or a valid path then we should just log the
            # error
            log.error("Unable to check existence of the resource: {0}".format(url))
            raise ResourceError("Unable to access resource",
                "The resource was not found in the resource cache")

        return os.path.getsize(url)

    response = None
    try:
        response = requests.head(url)
    except Exception, e:
        log.error("Unable to access resource {0}: {1}".format(url, e))
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

def sizeof_fmt(num, decimal_places=1):
    '''Given a number of bytes, returns it in human-readable format.
    >>> sizeof_fmt(168963795964)
    '157.4GB'
    '''
    try:
        num = float(num)
    except ValueError:
        return num
    format_string = '%%3.%sf%%s' % decimal_places
    for x in ['bytes','KB','MB','GB']:
        if num < 1024.0:
            return format_string % (num, x)
        num /= 1024.0
    return format_string % (num, 'TB')


def _open_file(url):
    return open(url, 'r')


def _open_url(url):
    """ URLs with &pound; in, just so, so wrong. """
    try:
        return urllib2.urlopen(url.encode("utf-8"))
    except Exception, e:
        log.error("URL %s caused: %s" % (url, e))

    return None


def proxy_query(resource, url, query):
    '''
    Given the URL for a data file, return its transformed contents in JSON form.

    e.g. if it is a spreadsheet, it returns a JSON dict:
        {
            "fields": ['Name', 'Age'],
            "data": [['Bob', 42], ['Jill', 54]],
            "max_results": 10,
            "length": 435,
            "url": "http://data.com/file.csv",
        }
    Whatever it is, it always has length (file size in bytes) and url (where
    it got the data from, which might be a URL or a local cache filepath).

    :param resource: resource object
    :param url: URL or local filepath
    :param query: dict about the URL:
          type - (optional) format of the file - extension or mimetype.
                            Only specify this if you the caller knows better
                            than magic can detect it.
                            Defaults to the file extension of the URL.
          length - (optional) size of the file. If not supplied,
                              it will determine it.
          size_limit - max size of the file to transform
          indent - (optional) the indent for the pprint the JSON result
    '''
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

    length = query.get('length',
                       get_resource_length(url,
                                           trans.requires_size_limit))

    log.debug('The file at %s has length %s', url, length)

    max_length = int(query['size_limit'])

    if length and trans.requires_size_limit and int(length) > max_length:
        raise ResourceError('The requested file is too large to preview',
                            'Requested resource is %s. '
                            'Size limit is %s. '
                            % (sizeof_fmt(length),
                               sizeof_fmt(max_length, decimal_places=0)))

    try:
        result = trans.transform()
    except ResourceError, reserr:
        log.debug('Transformation of %s failed. %s', url, reserr)        
        raise reserr
    except StopIteration as si:
        # In all likelihood, there was no data to read
        log.debug('Transformation of %s failed. %s', url, si)
        raise ResourceError("Data Transformation Error",
            "There was a problem reading the resource data")
    except Exception, e:
        log.exception(e)
        raise ResourceError("Data Transformation Error",
            "Data transformation failed. %s" % (e))

    indent = None

    if url.startswith('http'):
        result["url"] = url
    else:
        result["url"] = resource.cache_url or resource.url

    result["length"] = length or 0

    if 'indent' in query:
        indent = int(query.getfirst('indent'))

    return json.dumps(result, indent=indent)

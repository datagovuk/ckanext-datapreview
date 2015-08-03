import re
import os
import urlparse
import urllib
import urllib2
import logging
import json
from requests import Session, Request
from ckanext.datapreview.transform.base import transformer
from ckanext.datapreview.lib.errors import (ResourceError, RequestError)

log = logging.getLogger('ckanext.datapreview.lib.helpers')

REDIRECT_LIMIT = 3


def get_resource_length(url, resource, required=False, redirects=0):
    '''Get file size of a resource.

    Either do a HEAD request to the url, or checking the
    size on disk. (Will not download the whole file.)

    :param url: URL to check
    :param resource: Resource object, just for identification purposes
    :param required: If cannot get the length (without resorting to downloading
                     the whole file), raises ResourceError
    :param redirects: For counting the number of recursions due to redirects

    On error, this method raises ResourceError.

    If the headers do not contain the length, this method returns None.
    '''
    log.debug('Getting resource length of %s' % url)
    # Case 1: url is a filename
    if not url.startswith('http'):
        try:
            if not os.path.exists(url):
                raise ResourceError("Unable to access resource",
                    "The resource was not found in the resource cache: %s" % \
                                    identify_resource(resource))
        except:
            # If the URL is neither http:// or a valid path then we should just log the
            # error
            log.info(u"Unable to check existence of the resource: {0}".format(url))
            raise ResourceError("Unable to access resource",
                "The resource was not found in the resource cache: %s" % \
                                    identify_resource(resource))

        return os.path.getsize(url)

    # Case 2: url is a URL. Send a HEAD to get the Content-Length
    response = None
    # NB requests doesn't receive the Content-Length header from servers
    # including ours unless you tell it not to send the Content-Length header
    # in the request (!)
    # e.g. http://data.gov.uk/data/resource_cache/2a/2ac8abba-4a71-4f12-af1b-57ad0e36b6a4/MOTsitelist.csv
    s = Session()
    req = Request('HEAD', url)
    prepped_request = req.prepare()
    if 'Content-Length' in prepped_request.headers:
        del prepped_request.headers['Content-Length']
    try:
        # verify=False means don't verify the SSL certificate
        response = s.send(prepped_request, verify=False)
    except Exception, e:
        log.info("Unable to access resource {0}: {1}".format(url, e))
        raise ResourceError("Unable to access resource",
            "There was a problem retrieving the resource URL %s : %s" % \
                                    (identify_resource(resource), e))

    headers = {}
    for header, value in response.headers.iteritems():
        headers[header.lower()] = value

    # Redirect?
    # DR: requests handles redirects, so this section may be removed
    if response.status_code == 302 and redirects < REDIRECT_LIMIT:
        if "location" not in headers:
            raise ResourceError("Resource moved, but no Location provided by resource server",
                'Resource %s moved, but no Location provided by resource server: %s' % \
                                (url, identify_resource(resource)))

        # if our redirect location is relative, then we can only assume
        # it is relative to the url we've just requested.
        if not headers['location'].startswith('http'):
            loc = urlparse.urljoin(url, headers['location'])
        else:
            loc = headers['location']

        return get_resource_length(loc, resource, required=required,
            redirects=redirects + 1)

    if 'content-length' in headers:
        length = int(headers['content-length'])
        return length

    if required:
        log.info('No content-length returned for server: %s'
                                % (url))
        raise ResourceError("Unable to get content length",
            'Unable to find the size of the remote resource: %s' % \
                                identify_resource(resource))
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
    """ URLs with &pound; in, just so, so wrong.

    Errors fetching the URL are ignored.
    """
    try:
        return urllib2.urlopen(url.encode("utf-8"))
    except Exception, e:
        log.info("URL %s caused: %s" % (url, e))

    return None


def proxy_query(resource, url, query):
    '''
    Given the URL for a data file, return its transformed contents in JSON form.

    e.g. if it is a spreadsheet, it returns a JSON dict:
        {
            "archived": "This file is previewed from the data.gov.uk archive.",
            "fields": ['Name', 'Age'],
            "data": [['Bob', 42], ['Jill', 54]],
            "extra_text": "This preview shows only the first 10 rows",
            "max_results": 10,
            "length": 435,
            "url": "http://data.com/file.csv",
        }
    Whatever it is, it always has length (file size in bytes) and url (where
    it got the data from, which might be a URL or a local cache filepath).

    Or an error message:
        {
            "error": {
                "message": "Requested resource is 21.3MB. Size limit is  19MB. Resource: /dataset/your-freedom-data/resource/ea11ed1e-d793-4fc6-b150-fb362a7ccac9",
                "title": "The requested file is too large to preview"
            }
        }

    May raise RequestError.

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
                       get_resource_length(url, resource,
                                           trans.requires_size_limit()))

    log.debug('The file at %s has length %s', url, length)

    max_length = int(query['size_limit'])

    transformer_requires_size_limit = trans.requires_size_limit()
    log.debug('Size=%s Archived=%s Transformer-limited=%s Limit=%s',
              int(length), query.get('archived'),
              transformer_requires_size_limit, max_length)
    if query.get('archived', True) and not transformer_requires_size_limit:
        log.debug('Skipping size check - reading from archive and the '
                  'transformer for this format does not require a limit')
    else:
        # Do size check
        #  - remote files may take too long to download
        #  - some file formats need loading fully into memory and take too much
        if length and int(length) > max_length:
            raise ResourceError('The requested file is too large to preview',
                                'Requested resource is %s. '
                                'Size limit is %s. Resource: %s'
                                % (sizeof_fmt(length),
                                   sizeof_fmt(max_length, decimal_places=0),
                                   identify_resource(resource)))

    try:
        result = trans.transform()
    except ResourceError, reserr:
        log.debug('Transformation of %s failed. %s', url, reserr)
        raise reserr
    except StopIteration as si:
        # In all likelihood, there was no data to read
        log.debug('Transformation of %s failed. %s', url, si)
        raise ResourceError("Data Transformation Error",
            "There was a problem reading the resource data: %s" %
                                    identify_resource(resource))
    except Exception, e:
        log.exception(e)
        raise ResourceError("Data Transformation Error",
                            "Data transformation failed: %s %s" %
                                    (identify_resource(resource), e))

    indent = None

    if url.startswith('http'):
        result["url"] = url
    else:
        result["url"] = resource.cache_url or resource.url

    result["length"] = length or 0

    if 'indent' in query:
        indent = int(query.getfirst('indent'))

    return json.dumps(result, indent=indent)

def identify_resource(resource):
    '''Returns a printable identity of a resource object.
    e.g. '/dataset/energy-data/d1bedaa1-a1a3-462d-9a25-7b39a941d9f9'
    '''
    dataset_name = resource.resource_group.package.name if resource.resource_group else '?'
    return '/dataset/{0}/resource/{1}'.format(dataset_name, resource.id)

def fix_url(url):
    """
    Any Unicode characters in a URL become encoded in UTF8.
    It does this by unquoting, encoding as UTF8, and quoting again.

    It must not get tripped up on '+' characters which are encoded spaces.
    e.g. This should be left unchanged or the "+" changed to "%20" :
      http://data.defra.gov.uk/inspire/UK+MR_indicators_Report_2011V6.csv
    """
    from requests.exceptions import InvalidURL

    # resource.url is type unicode, but if it isn't for some reason, decode
    if not isinstance(url, unicode):
        url = url.decode('utf8')

    # parse it
    parsed = urlparse.urlsplit(url)

    # divide the netloc further
    userpass, at, hostport = parsed.netloc.rpartition('@')
    user, colon1, pass_ = userpass.partition(':')
    host, colon2, port = hostport.partition(':')

    def fix_common_host_problems(host):
        return host.replace('..', '.')

    # encode each component
    scheme = parsed.scheme.encode('utf8')
    user = urllib.quote(user.encode('utf8'))
    colon1 = colon1.encode('utf8')
    pass_ = urllib.quote(pass_.encode('utf8'))
    at = at.encode('utf8')

    try:
        host = fix_common_host_problems(host).encode('idna')
    except UnicodeError:
        # This is an invalid URL, so we should complain by abusing the
        # requests InvalidUrl exception
        raise InvalidURL("URL is not valid")

    colon2 = colon2.encode('utf8')
    port = port.encode('utf8')
    path = '/'.join(  # could be encoded slashes!
        urllib.quote(urllib.unquote_plus(pce).encode('utf8'),'')
        for pce in parsed.path.split('/')
    )
    query = urllib.quote(urllib.unquote(parsed.query).encode('utf8'),'=&?/')
    fragment = urllib.quote(urllib.unquote(parsed.fragment).encode('utf8'))

    # put it back together
    netloc = ''.join((user,colon1,pass_,at,host,colon2,port))
    return urlparse.urlunsplit((scheme,netloc,path,query,fragment))

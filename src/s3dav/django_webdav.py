# Copyright (c) 2011, SmartFile <btimby@smartfile.com>
# All rights reserved.
#
# This file is part of django-webdav.
#
# Foobar is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Foobar is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with django-webdav.  If not, see <http://www.gnu.org/licenses/>.
import sys
import os, datetime, mimetypes, time, shutil, urllib, urlparse, httplib, re, calendar
from xml.etree import ElementTree
from django.conf import settings
from django.http import HttpResponse, HttpResponseForbidden, HttpResponseNotFound, \
HttpResponseNotAllowed, HttpResponseBadRequest, HttpResponseNotModified
from django.http import Http404 as HttpNotFound
from django.utils import hashcompat, synch
from django.utils.http import http_date, parse_etags
from django.utils.encoding import smart_unicode
from django.shortcuts import render_to_response
try:
    from email.utils import parsedate_tz
except ImportError:
    from email.Utils import parsedate_tz

PATTERN_IF_DELIMITER = re.compile(r'(\<([^>]+)\>)|(\(([^\)]+)\))')

# Sun, 06 Nov 1994 08:49:37 GMT  ; RFC 822, updated by RFC 1123
FORMAT_RFC_822 = '%a, %d %b %Y %H:%M:%S GMT'
# Sunday, 06-Nov-94 08:49:37 GMT ; RFC 850, obsoleted by RFC 1036
FORMAT_RFC_850 = '%A %d-%b-%y %H:%M:%S GMT'
# Sun Nov  6 08:49:37 1994       ; ANSI C's asctime() format
FORMAT_ASC = '%a %b %d %H:%M:%S %Y'

def safe_join(root, *paths):
    '''The provided os.path.join() does not work as desired. Any path starting with /
    will simply be returned rather than actually being joined with the other elements.'''
    if not root.startswith('/'):
        root = '/' + root
    for path in paths:
        while root.endswith('/'):
            root = root[:-1]
        while path.startswith('/'):
            path = path[1:]
        root += '/' + path
    return root

def url_join(base, *paths):
    '''Assuming base is the scheme and host (and perhaps path) we will join the remaining
    path elements to it.'''
    paths = safe_join(*paths)
    while base.endswith('/'):
        base = base[:-1]
    return base + paths

def ns_split(tag):
    '''Splits the namespace and property name from a clark notation property name.'''
    if tag.startswith("{") and "}" in tag:
        ns, name = tag.split("}", 1)
        return (ns[1:-1], name)
    return ("", tag)

def ns_join(ns, name):
    '''Joins a namespace and property name into clark notation.'''
    return '{%s:}%s' % (ns, name)

def rfc3339_date(date):
  if not date:
      return ''
  if not isinstance(date, datetime.date):
      date = datetime.date.fromtimestamp(date)
  date = date + datetime.timedelta(seconds=-time.timezone)
  if time.daylight:
    date += datetime.timedelta(seconds=time.altzone)
  return date.strftime('%Y-%m-%dT%H:%M:%SZ')

def parse_time(time):
    value = None
    for fmt in (FORMAT_RFC_822, FORMAT_RFC_850, FORMAT_ASC):
        try:
            value = time.strptime(timestring, fmt)
        except:
            pass
    if value is None:
        try:
            # Sun Nov  6 08:49:37 1994 +0100      ; ANSI C's asctime() format with timezone
            value = parsedate_tz(timestring)
        except:
            pass
    if value is None:
        return
    return calendar.timegm(result)


# When possible, code returns an HTTPResponse sub-class. In some situations, we want to be able
# to raise an exception to control the response (error conditions within utility functions). In
# this case, we provide HttpError sub-classes for raising.


class HttpError(Exception):
    '''A base HTTP error class. This allows utility functions to raise an HTTP error so that
    when used inside a handler, the handler can simply call the utility and the correct 
    HttpResponse will be issued to the client.'''
    status_code = 500

    def get_response(self):
        '''Creates an HTTPResponse for the given status code.'''
        return HttpResponse(self.message, status=self.status_code)


class HttpCreated(HttpError):
    status_code = httplib.CREATED


class HttpResponseCreated(HttpResponse):
    status_code = httplib.CREATED


class HttpNoContent(HttpError):
    status_code = httplib.NO_CONTENT


class HttpResponseNoContent(HttpResponse):
    status_code = httplib.NO_CONTENT


class HttpMultiStatus(HttpError):
    status_code = httplib.MULTI_STATUS


class HttpResponseMultiStatus(HttpResponse):
    status_code = httplib.MULTI_STATUS


class HttpNotAllowed(HttpError):
    status_code = httplib.METHOD_NOT_ALLOWED


class HttpResponseNotAllowed(HttpResponse):
    status_code = httplib.METHOD_NOT_ALLOWED


class HttpConflict(HttpError):
    status_code = httplib.CONFLICT


class HttpResponseConflict(HttpResponse):
    status_code = httplib.CONFLICT


class HttpPreconditionFailed(HttpError):
    status_code = httplib.PRECONDITION_FAILED


class HttpResponsePreconditionFailed(HttpResponse):
    status_code = httplib.PRECONDITION_FAILED


class HttpMediatypeNotSupported(HttpError):
    status_code = httplib.UNSUPPORTED_MEDIA_TYPE


class HttpResponseMediatypeNotSupported(HttpResponse):
    status_code = httplib.UNSUPPORTED_MEDIA_TYPE

class HttpNotImplemented(HttpError):
    status_code = httplib.NOT_IMPLEMENTED


class HttpBadGateway(HttpError):
    status_code = httplib.BAD_GATEWAY


class HttpResponseBadGateway(HttpResponse):
    status_code = httplib.BAD_GATEWAY

class HttpBadRequest(HttpError):
    status_code = httplib.BAD_REQUEST



class DavAcl(object):
    '''Represents all the permissions that a user might have on a resource. This
    makes it easy to implement virtual permissions.'''
    def __init__(self, read=True, write=True, delete=True, create=True, relocate=True, list=True, all=None):
        if not all is None:
            self.read = self.write = self.delete = \
            self.create = self.relocate = self.list = all
        self.read = read
        self.write = write
        self.delete = delete
        self.create = create
        self.relocate = relocate
        self.list = list


class DavResource(object):
    '''Implements an interface to the file system. This can be subclassed to provide
    a virtual file system (like say in MySQL). This default implementation simply uses
    python's os library to do most of the work.'''
    def __init__(self, server, path):
        self.server = server
        self.root = server.get_root()
        # Trailing / messes with dirname and basename.
        while path.endswith('/'):
            path = path[:-1]
        self.path = path

    def get_path(self):
        '''Return the path of the resource relative to the root.'''
        return self.path

    def get_abs_path(self):
        '''Return the absolute path of the resource. Used internally to interface with
        an actual file system. If you override all other methods, this one will not
        be used.'''
        abspath = safe_join(self.root, self.path)
        return abspath

    def isdir(self):
        '''Return True if this resource is a directory (collection in WebDAV parlance).'''
        return os.path.isdir(self.get_abs_path())

    def isfile(self):
        '''Return True if this resource is a file (resource in WebDAV parlance).'''
        return os.path.isfile(self.get_abs_path())

    def exists(self):
        '''Return True if this resource exists.'''
        return os.path.exists(self.get_abs_path())

    def get_name(self):
        '''Return the name of the resource (without path information).'''
        # No need to use absolute path here
        return os.path.basename(self.path)

    def get_dirname(self):
        '''Return the resource's parent directory's absolute path.'''
        return os.path.dirname(self.get_abs_path())

    def get_size(self):
        '''Return the size of the resource in bytes.'''
        return os.path.getsize(self.get_abs_path())

    def get_ctime_stamp(self):
        '''Return the create time as UNIX timestamp.'''
        return os.stat(self.get_abs_path()).st_ctime

    def get_ctime(self):
        '''Return the create time as datetime object.'''
        return datetime.datetime.fromtimestamp(self.get_ctime_stamp())

    def get_mtime_stamp(self):
        '''Return the modified time as UNIX timestamp.'''
        return os.stat(self.get_abs_path()).st_mtime

    def get_mtime(self):
        '''Return the modified time as datetime object.'''
        return datetime.datetime.fromtimestamp(self.get_mtime_stamp())

    def get_url(self):
        '''Return the url of the resource. This uses the request base url, so it
        is likely to work even for an overridden DavResource class.'''
        return url_join(self.server.request.get_base_url(), self.path)

    def get_parent(self):
        '''Return a DavResource for this resource's parent.'''
        return self.__class__(self.server, os.path.dirname(self.path))

    # TODO: combine this and get_children()
    def get_descendants(self, depth=1, include_self=True):
        '''Return an iterator of all descendants of this resource.'''
        if include_self:
            yield self
        # If depth is less than 0, then it started out as -1.
        # We need to keep recursing until we hit 0, or forever
        # in case of infinity.
        if depth != 0:
            for child in self.get_children():
                for desc in child.get_descendants(depth=depth-1, include_self=True):
                    yield desc

    # TODO: combine this and get_descendants()
    def get_children(self):
        '''Return an iterator of all direct children of this resource.'''
        for child in os.listdir(self.get_abs_path()):
            yield self.__class__(self.server, os.path.join(self.get_path(), child))

    def open(self, mode):
        '''Open the resource, mode is the same as the Python file() object.'''
        return open(self.get_abs_path(), mode)

    def delete(self):
        '''Delete the resource, recursive is implied.'''
        if self.isdir():
            for child in self.get_children():
                child.delete()
            try:
                os.rmdir(self.get_abs_path())
            except OSError:
                import traceback
                traceback.print_exc()

        elif self.isfile():
            try:
                os.remove(self.get_abs_path())
            except OSError:
                import traceback
                traceback.print_exc()

    def mkdir(self):
        '''Create a directory in the location of this resource.'''
        os.mkdir(self.get_abs_path())

    def copy(self, destination, depth=0):
        '''Called to copy a resource to a new location. Overwrite is assumed, the DAV server
        will refuse to copy to an existing resource otherwise. This method needs to gracefully
        handle a pre-existing destination of any type. It also needs to respect the depth 
        parameter. depth == -1 is infinity.'''
        if self.isdir():
            if destination.isfile():
                destination.delete()
            if not destination.isdir():
                destination.mkdir()
            # If depth is less than 0, then it started out as -1.
            # We need to keep recursing until we hit 0, or forever
            # in case of infinity.
            if depth != 0:
                for child in self.get_children():
                    child.copy(self.__class__(self.server, safe_join(destination.get_path(), child.get_name())), depth=depth-1)
        else:
            if destination.isdir():
                destination.delete()
            shutil.copy(self.get_abs_path(), destination.get_abs_path())

    def move(self, destination):
        '''Called to move a resource to a new location. Overwrite is assumed, the DAV server
        will refuse to move to an existing resource otherwise. This method needs to gracefully
        handle a pre-existing destination of any type.'''
        if destination.exists():
            destination.delete()
        if self.isdir():
            destination.mkdir()
            for child in self.get_children():
                child.move(self.__class__(self.server, safe_join(destination.get_path(), child.get_name())))
            self.delete()
        else:
            os.rename(self.get_abs_path(), destination.get_abs_path())

    def get_etag(self):
        '''Calculate an etag for this resource. The default implementation uses an md5 sub of the
        absolute path modified time and size. Can be overridden if resources are not stored in a
        file system. The etag is used to detect changes to a resource between HTTP calls. So this
        needs to change if a resource is modified.'''
        hash = hashcompat.md5_constructor()
        hash.update(self.get_abs_path().encode('utf-8'))
        hash.update(str(self.get_mtime_stamp()))
        hash.update(str(self.get_size()))
        return hash.hexdigest()


class DavRequest(object):
    '''Wraps a Django request object, and extends it with some WebDAV
    specific methods.'''
    def __init__(self, server, request, path):
        self.server = server
        self.request = request
        self.path = path

    def __getattr__(self, name):
        return getattr(self.request, name)

    def get_base(self):
        '''Assuming the view is configured via urls.py to pass the path portion using
        a regular expression, we can subtract the provided path from the full request
        path to determine our base. This base is what we can make all absolute URLs
        from.'''
        return self.META['PATH_INFO'][:-len(self.path)]

    def get_base_url(self):
        '''Build a base URL for our request. Uses the base path provided by get_base()
        and the scheme/host etc. in the request to build a URL that can be used to
        build absolute URLs for WebDAV resources.'''
        return self.build_absolute_uri(self.get_base())


class DavProperty(object):
    LIVE_PROPERTIES = [
        '{DAV:}getetag', '{DAV:}getcontentlength', '{DAV:}creationdate',
        '{DAV:}getlastmodified', '{DAV:}resourcetype', '{DAV:}displayname'
    ]

    def __init__(self, server):
        self.server = server
        self.lock = synch.RWLock()

    def get_dead_names(self, res):
        return []

    def get_dead_value(self, res, name):
        '''Implements "dead" property retrival. Thread synchronization is handled outside this method.'''
        return

    def set_dead_value(self, res, name, value):
        '''Implements "dead" property storage. Thread synchronization is handled outside this method.'''
        return

    def del_dead_prop(self, res, name):
        '''Implements "dead" property removal. Thread synchronizatioin is handled outside this method.'''
        return

    def get_prop_names(self, res, *names):
        return self.LIVE_PROPERTIES + self.get_dead_names(res)

    def get_prop_value(self, res, name):
        self.lock.reader_enters()
        try:
            ns, bare_name = ns_split(name)

            if ns != 'DAV':
                return self.get_dead_value(res, name)
            else:
                value = None
                if bare_name == 'getetag':
                    value = res.get_etag()
                elif bare_name == 'getcontentlength':
                    value = str(res.get_size())
                elif bare_name == 'creationdate':
                    value = rfc3339_date(res.get_ctime_stamp())     # RFC3339:
                elif bare_name == 'getlastmodified':
                    t = res.get_mtime_stamp()
                    value = http_date(t)
                elif bare_name == 'resourcetype':
                    if res.isdir():
                        value = []
                    else:
                        value = ''
                elif bare_name == 'displayname':
                    value = res.get_name()
                elif bare_name == 'href':
                    value = res.get_url()
            return value
        finally:
            self.lock.reader_leaves()

    def set_prop_value(self, res, name, value):
        self.lock.writer_enters()
        try:
            ns, bare_name = ns_split(name)
            if ns == 'DAV':
                pass # TODO: handle set-able "live" properties?
            else:
                self.set_dead_value(res, name, value)
        finally:
            self.lock.writer_leaves()

    def del_props(self, res, *names):
        self.lock.writer_enters()
        try:
            avail_names = self.get_prop_names(res)
            if not names:
                names = avail_names
            for name in names:
                ns, bare_name = ns_split(name)
                if ns == 'DAV':
                    pass # TODO: handle delete-able "live" properties?
                else:
                    self.del_dead_prop(res, name)
        finally:
            self.lock.writer_leaves()

    def copy_props(self, src, dst, *names, **kwargs):
        move = kwargs.get('move', False)
        self.lock.writer_enters()
        try:
            names = self.get_prop_names(src)
            for name in names:
                ns, bare_name = ns_split(name)
                if ns == 'DAV':
                    continue
                self.set_dead_value(dst, name, self.get_prop_value(src, name))
                if move:
                    self.del_dead_prop(self, name)
        finally:
            self.lock.writer_leaves()

    def get_propstat(self, res, el, *names):
        '''Returns the XML representation of a resource's properties. Thread synchronization is handled
        in the get_prop_value() method individually for each property.'''
        el404, el200 = None, None
        avail_names = self.get_prop_names(res)
        if not names:
            names = avail_names
        for name in names:
            if name in avail_names:
                value = self.get_prop_value(res, name)
                if el200 is None:
                    el200 = ElementTree.SubElement(el, '{DAV:}propstat')
                    ElementTree.SubElement(el200, '{DAV:}status').text = 'HTTP/1.1 200 OK'
                prop = ElementTree.SubElement(el200, '{DAV:}prop')
                prop = ElementTree.SubElement(prop, name)
                if isinstance(value, list):
                    prop.append(ElementTree.Element("{DAV:}collection"))
                elif value:
                    prop.text = smart_unicode(value)
            else:
                if el404 is None:
                    el404 = ElementTree.SubElement(el, '{DAV:}propstat')
                    ElementTree.SubElement(el404, '{DAV:}status').text = 'HTTP/1.1 404 Not Found'
                prop = ElementTree.SubElement(el404, '{DAV:}prop')
                prop = ElementTree.SubElement(prop, name)


class DavLock(object):
    def __init__(self, server):
        self.server = server
        self.lock = synch.RWLock()

    def get(self, res):
        '''Gets all active locks for the requested resource. Returns a list of locks.'''
        self.lock.reader_enters()
        try:
            pass
        finally:
            self.lock.reader_leaves()

    def acquire(self, res, type, scope, depth, owner, timeout):
        '''Creates a new lock for the given resource.'''
        self.lock.writer_enters()
        try:
            pass
        finally:
            self.lock.writer_leaves()

    def release(self, lock):
        '''Releases the lock referenced by the given lock id.'''
        self.lock.writer_enters()
        try:
            pass
        finally:
            self.lock.writer_leaves()

    def del_locks(self, res):
        '''Releases all locks for the given resource.'''
        self.lock.writer_enters()
        try:
            pass
        finally:
            self.lock.writer_leaves()


class DavServer(object):
    def __init__(self, request, path, property_class=DavProperty, resource_class=DavResource, lock_class=DavLock, acl_class=DavAcl):
        self.request = DavRequest(self, request, path)
        self.resource_class = resource_class
        self.acl_class = acl_class
        self.props = property_class(self)
        self.locks = lock_class(self)
        self.time_s = time.time()

    def get_root(self):
        '''Return the root of the file system we wish to export. By default the root
        is read from the DAV_ROOT setting in django's settings.py. You can override
        this method to export a different directory (maybe even different per user).'''
        return getattr(settings, 'DAV_ROOT', None)

    def get_access(self, path):
        '''Return permission as DavAcl object. A DavACL should have the following attributes:
        read, write, delete, create, relocate, list. By default we implement a read-only
        system.'''
        return self.acl_class(list=True, read=True, all=False)

    def get_resource(self, path):
        '''Return a DavResource object to represent the given path.'''
        return self.resource_class(self, path)

    def get_depth(self, default='infinity'):
        depth = self.request.META.get('HTTP_DEPTH', default).lower()
        if not depth in ('0', '1', 'infinity'):
            raise HttpBadRequest('Invalid depth header value %s' % depth)
        if depth == 'infinity':
            depth = -1
        else:
            depth = int(depth)
        return depth

    def evaluate_conditions(self, res):
        if not res.exists():
            return
        etag = res.get_etag()
        mtime = res.get_mtime_stamp()
        cond_if_match = self.request.META.get('HTTP_IF_MATCH', None)
        if cond_if_match:
            etags = parse_etags(cond_if_match)
            if '*' in etags or etag in etags:
                raise HttpPreconditionFailed()
        cond_if_modified_since = self.request.META.get('HTTP_IF_MODIFIED_SINCE', False)
        if cond_if_modified_since:
            # Parse and evaluate, but don't raise anything just yet...
            # This might be ignored based on If-None-Match evaluation.
            cond_if_modified_since = parse_time(cond_if_modified_since)
            if cond_if_modified_since and cond_if_modified_since > mtime:
                cond_if_modified_since = True
            else:
                cond_if_modified_since = False
        cond_if_none_match = self.request.META.get('HTTP_IF_NONE_MATCH', None)
        if cond_if_none_match:
            etags = parse_etags(cond_if_none_match)
            if '*' in etags or etag in etags:
                if self.request.method in ('GET', 'HEAD'):
                    raise HttpNotModified()
                raise HttpPreconditionFailed()
            # Ignore If-Modified-Since header...
            cond_if_modified_since = False
        cond_if_unmodified_since = self.request.META.get('HTTP_IF_UNMODIFIED_SINCE', None)
        if cond_if_unmodified_since:
            cond_if_unmodified_since = parse_time(cond_if_unmodified_since)
            if cond_if_unmodified_since and cond_if_unmodified_since <= mtime:
                raise HttpPreconditionFailed()
        if cond_if_modified_since:
            # This previously evaluated True and is not being ignored...
            raise HttpNotModified()
        # TODO: complete If header handling...
        cond_if = self.request.META.get('HTTP_IF', None)
        if cond_if:
            if not cond_if.startswith('<'):
                cond_if = '<*>' + cond_if
            #for (tmpurl, url, tmpcontent, content) in PATTERN_IF_DELIMITER.findall(cond_if):
                

    def get_response(self):
        handler = getattr(self, 'do' + self.request.method, None)
        try:
            if not callable(handler):
                raise HttpNotAllowed()
            return handler()
        except HttpError, e:
            import traceback
            traceback.print_exc()
            return e.get_response()
        except Exception, e:
            import traceback
            traceback.print_exc()
            return HttpError(str(e)).get_response()

    def doGET(self, head=False):
        res = self.get_resource(self.request.path)
        acl = self.get_access(res.get_abs_path())
        if not head and res.isdir():
            if not acl.list:
                return HttpResponseForbidden()
            return render_to_response('webdav/index.html', { 'res': res })
        else:
            if not acl.read:
                return HttpResponseForbidden()
            if head and res.exists():
                response = HttpResponse()
            elif head:
                response = HttpResponseNotFound()
            else:
                use_sendfile = getattr(settings, 'DAV_USE_SENDFILE', '').split()
                if len(use_sendfile) > 0 and use_sendfile[0].lower() == 'x-sendfile':
                    full_path = res.get_abs_path().encode('utf-8')
                    if len(use_sendfile) == 2 and use_sendfile[1] == 'escape':
                        full_path = urllib.quote(full_path)
                    response = HttpResponse()
                    response['X-SendFile'] = full_path
                elif len(use_sendfile) == 2 and use_sendfile[0].lower() == 'x-accel-redir':
                    full_path = res.get_abs_path().encode('utf-8')
                    full_path = url_join(use_sendfile[1], full_path)
                    response = HttpResponse()
                    response['X-Accel-Redirect'] = full_path
                    response['X-Accel-Charset'] = 'utf-8'
                else:
                    # Do things the slow way:
                    response =  HttpResponse(res.open('r'))
            if res.exists():
                response['Content-Type'] = mimetypes.guess_type(res.get_name())
                response['Content-Length'] = res.get_size()
                response['Last-Modified'] = http_date(res.get_mtime_stamp())
                response['ETag'] = res.get_etag()
            response['Date'] = http_date()
        return response

    def doHEAD(self):
        return self.doGET(head=True)

    def doPOST(self):
        return HttpResponseNotAllowed('POST method not allowed')

    def doPUT(self):
        res = self.get_resource(self.request.path)
        if res.isdir():
            return HttpResponseNotAllowed()
        if not res.get_parent().exists():
            return HttpResponseNotFound()
        acl = self.get_access(res.get_abs_path())
        if not acl.write:
            return HttpResponseForbidden()

        created = not res.exists()
        with res.open('w') as f:
            shutil.copyfileobj(self.request, f)
        
        if created:
            return HttpResponseCreated()
        else:
            return HttpResponseNoContent()

    def doDELETE(self):
        res = self.get_resource(self.request.path)
        if not res.exists():
            return HttpResponseNotFound()
        acl = self.get_access(res.get_abs_path())
        if not acl.delete:
            return HttpResponseForbidden()
        self.locks.del_locks(res)
        self.props.del_props(res)
        res.delete()
        response = HttpResponseNoContent()
        response['Date'] = http_date()
        return response

    def doMKCOL(self):
        res = self.get_resource(self.request.path)
        if res.exists():
            return HttpResponseNotAllowed()
        if not res.get_parent().exists():
            return HttpResponseConflict()
        length = self.request.META.get('CONTENT_LENGTH', 0)
        if length and int(length) != 0:
            return HttpResponseMediatypeNotSupported()
        acl = self.get_access(res.get_abs_path())
        if not acl.create:
            return HttpResponseForbidden()
        res.mkdir()
        return HttpResponseCreated()

    def doCOPY(self, move=False):
        res = self.get_resource(self.request.path)
        if not res.exists():
            return HtpResponseNotFound()
        acl = self.get_access(res.get_abs_path())
        if not acl.relocate:
            return HttpResponseForbidden()
        dst = urllib.unquote(self.request.META.get('HTTP_DESTINATION', ''))
        if not dst:
            return HttpResponseBadRequest('Destination header missing.')
        dparts = urlparse.urlparse(dst)
        # TODO: ensure host and scheme portion matches ours...
        sparts = urlparse.urlparse(self.request.build_absolute_uri())
        if sparts.scheme != dparts.scheme or sparts.netloc != dparts.netloc:
            return HttpResponseBadGateway('Source and destination must have the same scheme and host.')
        # adjust path for our base url:
        dst = self.get_resource(dparts.path[len(self.request.get_base()):])
        if not dst.get_parent().exists():
            return HttpResponseConflict()
        overwrite = self.request.META.get('HTTP_OVERWRITE', 'T')
        if overwrite not in ('T', 'F'):
            return HttpResponseBadRequest('Overwrite header must be T or F.')
        overwrite = (overwrite == 'T')
        if not overwrite and dst.exists():
            return HttpResponsePreconditionFailed('Destination exists and overwrite False.')
        depth = self.get_depth()
        if move and depth != -1:
            return HttpResponseBadRequest()
        if depth not in (0, -1):
            return HttpResponseBadRequest()
        dst_exists = dst.exists()
        if move:
            if dst_exists:
                self.locks.del_locks(dst)
                self.props.del_props(dst)
                dst.delete()
            errors = res.move(dst)
        else:
            errors = res.copy(dst, depth=depth)
        #print 'copy props', res, dst, self.props
        #self.props.copy(res, dst, move=move)
        if move:
            self.locks.del_locks(res)
        if errors:
            response = HttpResponseMultiStatus()
        elif dst_exists:
            response = HttpResponseNoContent()
        else:
            response = HttpResponseCreated()
        return response

    def doMOVE(self):
        return self.doCOPY(move=True)

    def doLOCK(self):
        raise HttpNotImplemented()

    def doUNLOCK(self):
        raise HttpNotImplemented()

    def doOPTIONS(self):
        response = HttpResponse(mimetype='text/html')
        response['DAV'] = '1,2'
        response['Date'] = http_date()
        if self.request.path in ('/', '*'):
            return response
        res = self.get_resource(self.request.path)
        acl = self.get_access(res.get_abs_path())
        if not res.exists():
            res = res.get_parent()
            if not res.isdir():
                return HttpResponseNotFound()
            response['Allow'] = 'OPTIONS PUT MKCOL'
        elif res.isdir():
            response['Allow'] = 'OPTIONS HEAD GET DELETE PROPFIND PROPPATCH COPY MOVE LOCK UNLOCK'
        else:
            response['Allow'] = 'OPTIONS HEAD GET PUT DELETE PROPFIND PROPPATCH COPY MOVE LOCK UNLOCK'
            response['Allow-Ranges'] = 'bytes'
        return response

    def doPROPFIND(self):
        res = self.get_resource(self.request.path)
        if not res.exists():
            return HttpResponseNotFound()
        acl = self.get_access(res.get_abs_path())
        if not acl.list:
            print >>sys.stderr, 'No acl'
            return HttpResponseForbidden()
        depth = self.get_depth()
        names_only, props = False, []
        length = self.request.META.get('CONTENT_LENGTH', 0)
        if not length or int(length) != 0:
            #Otherwise, empty prop list is treated as request for ALL props.
            for ev, el in ElementTree.iterparse(self.request):
                if el.tag == '{DAV:}allprop':
                    if props:
                        return HttpResponseBadRequest()
                elif el.tag == '{DAV:}propname':
                    names_only = True
                elif el.tag == '{DAV:}prop':
                    if names_only:
                        return HttpResponseBadRequest()
                    for pr in el:
                        props.append(pr.tag)
        msr = ElementTree.Element('{DAV:}multistatus')
        for child in res.get_descendants(depth=depth, include_self=True):
            response = ElementTree.SubElement(msr, '{DAV:}response')
            ElementTree.SubElement(response, '{DAV:}href').text = child.get_url()
            self.props.get_propstat(child, response, *props)
        response = HttpResponseMultiStatus(ElementTree.tostring(msr, 'UTF-8'), mimetype='application/xml')
        response['Date'] = http_date()
        return response

    def doPROPPATCH(self):
        res = self.get_resource(self.request.path)
        if not res.exists():
            return HttpResponseNotFound()
        depth = self.get_depth(default=0)
        if depth != 0:
            return HttpResponseBadRequest('Invalid depth header value %s' % depth)

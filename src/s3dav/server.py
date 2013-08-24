import boto
import os
import shutil
import tempfile
import time
import boto.s3.connection
import boto.s3.key
from dateutil import parser
import re
from django.http import HttpResponse, HttpResponseForbidden, HttpResponseNotFound, \
HttpResponseNotAllowed, HttpResponseBadRequest, HttpResponseNotModified
from django.conf import settings
from s3dav.django_webdav import DavServer, DavResource, safe_join, HttpResponseNoContent
from s3dav.django_webdav import HttpResponseCreated, url_join
import s3dav.django_webdav as dw

S3_CACHE_DIR=getattr(settings, 'S3_CACHE_DIR', os.path.abspath('cache'))

_conn_pool = {}
def connect_boto(aws_key, aws_secret):
    #boto.set_stream_logger('boto')
    s3 = _conn_pool.get(aws_key)
    if s3 is None:
        s3 =  boto.connect_s3(aws_access_key_id=aws_key,
                              aws_secret_access_key=aws_secret,
                              host=settings.AWS_HOST,
                              port=settings.AWS_PORT,
                              is_secure=False,
                              calling_format=boto.s3.connection.OrdinaryCallingFormat())
        _conn_pool[aws_key] = s3
    return s3

class S3DavRootResource(DavResource):
    def __init__(self, server):
        super(S3DavRootResource, self).__init__(server, '/')

    def get_abs_path(self):
        return S3_CACHE_DIR

    def get_url(self):
        return url_join(self.server.request.get_base_url(), '/')

    def isdir(self):
        return True

    def exists(self):
        return True

    def get_children(self):
        s3 = self.server.get_s3_connection()
        all_buckets = s3.get_all_buckets()
        for bucket in all_buckets:
            yield S3DavResource(self.server, bucket, None)

    def mkdir(self):
        s3 = self.server.get_s3_connection()
        s3.create_bucket()

        if not self.key_name.endswith('/'):
            self.key_name = self.key_name + '/'
        key = boto.s3.key.Key(bucket=self.bucket)
        key.key = self.key_name
        key.set_contents_from_string('')


class S3DavResource(DavResource):
    _parent_key = None

    def __init__(self, server, bucket, key, key_name=''):
        if key:
            path = '%s/%s' % (bucket.name, key.key)
        elif key_name:
            path = '%s/%s' % (bucket.name, key_name)
        else:
            path = '%s' % bucket.name
        super(S3DavResource, self).__init__(server, path)
        self.bucket = bucket
        self.key = key

        if self.key and not key_name:
            self.key_name = self.key.key
        else:
            self.key_name = key_name

    @property
    def cache_path(self):
        return safe_join(S3_CACHE_DIR, self.path)

    def get_abs_path(self):
        return self.cache_path

    def put_file(self):
        if self.key:
            key = self.key
        else:
            key = boto.s3.key.Key(bucket=self.bucket)
            key.key = self.key_name

        with self.open('u') as f:
            key.set_contents_from_file(f)

    def get_url(self):
        relpath = '/%s/%s' % (self.bucket.name, self.key_name)
        return url_join(self.server.request.get_base_url(), relpath)

    def open(self, mode):
        abspath = self.get_abs_path()
        if self.key and ('r' in mode):
            need_fetch = False
            if not os.path.exists(abspath):
                if self.isdir():
                    os.makedirs(abspath)
                else:
                    try:
                        os.makedirs(os.path.dirname(abspath))
                    except OSError:
                        import traceback
                        traceback.print_exc()
                    need_fetch = True
            elif not self.isdir() and os.stat(abspath).st_mtime < self.get_mtime_stamp():
                need_fetch = True

            if need_fetch:
                f = open(abspath, 'wb')
                self.key.get_contents_to_file(f)
                f.close()

        if mode == 'u':
            mode = 'r'

        if 'w' in mode:
            dirname = os.path.dirname(abspath)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
        return super(S3DavResource, self).open(mode)
        
    def isdir(self):
        if not self.exists():
            return False
        if not self.bucket:
            return True
        if not self.key:
            return True
        if self.key.key[-1:] == '/':
            return True
        return False

    def isfile(self):
        if not self.exists():
            return False
        return not self.isdir()

    def exists(self):
        if not self.bucket:
            return False
        if self.key:
            return True
        elif self.key_name:
            self.key = self.bucket.get_key(self.key_name)
            if not self.key:
                return False
        else:
            return True

    def get_children(self):
        children = []
        if not self.key:
            for key in self.bucket.get_all_keys():
                idx = key.key.find('/')
                if idx < 0 or idx == len(key.key) - 1:
                    children.append(self.__class__(self.server, self.bucket, key))
        elif self.isdir():
            for key in self.bucket.list(self.key.key):
                child_keyname = key.key[len(self.key.key):]
                if not child_keyname or child_keyname.startswith('/'):
                    continue
                children.append(self.__class__(self.server, self.bucket, key))
        #print 'children of', self.path, 'is', [(c.isdir(), c.path) for c in children]
        return children
    
    def get_parent(self):
        '''Return a DavResource for this resource's parent.'''
        if not self.key:
            return S3DavRootResource(self.server)
        if self._parent_key:
            return self._parent_key
        parent_key_name = re.sub(r'[^/]+/?$', '', self.key.key)
        if parent_key_name:
            self._parent_key = self.bucket.get_key(parent_key_name)
        else:
            self._parent_key = None
        return self.__class__(self.server, self.bucket, self._parent_key)

    def get_ctime_stamp(self):
        return self.get_mtime_stamp()

    def get_mtime_stamp(self):
        if self.key:
            d = parser.parse(self.key.last_modified)
            tm = int(time.mktime(d.timetuple()))
            return tm
        else:
            return int(time.time() - 1)

    def get_size(self):
        if self.key:
            return self.key.size
        return 0

    def get_etag(self):
        if self.key:
            return self.key.etag

    def delete(self):
        super(S3DavResource, self).delete()
        if self.key:
            self.key.delete()

    def mkdir(self):
        if not self.key_name.endswith('/'):
            self.key_name = self.key_name + '/'
        key = boto.s3.key.Key(bucket=self.bucket)
        key.key = self.key_name
        key.set_contents_from_string('')

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
            self.key.copy(destination.bucket.name, destination.key_name)

    def move(self, destination):
        if self.isdir():
            if destination.exist() and not destination.isdir():
                destination.delete()
            destination.mkdir()
            for child in self.get_children():
                child.move(self.__class__(self.server, safe_join(destination.get_path(), child.get_name())))
            self.delete()
        elif self.key:
            self.key.copy(destination.bucket.name, destination.key_name)
            self.key.delete()
        else:
            print 'No source key', self.key_name

class S3DavServer(DavServer):
    def __init__(self, request, path, **kw):
        super(S3DavServer, self).__init__(request, path, **kw)
        self.resource_class = S3DavResource

    def get_s3_connection(self):
        return connect_boto(self.request.aws_key, self.request.aws_secret)

    def get_access(self, path):
        if path in ('', '/', S3_CACHE_DIR):
            return self.acl_class(read=True, list=True)
        return self.acl_class(all=True)

    def doLOCK(self):
        # FIXME: implement an exclusive or shared lock
        return HttpResponse(status=200)

    def doUNLOCK(self):
        # FIXME: implement an exclusive or shared lock
        return HttpResponse(status=200)

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
        res.put_file()
        if created:
            return HttpResponseCreated()
        else:
            return HttpResponseNoContent()

    def get_resource(self, path):
        '''Return a DavResource object to represent the given path.'''
        if path in ('', '/'):
            return S3DavRootResource(self)
        m = re.match(r'/?(?P<bucket>[^/]+)/+(?P<key>.*)$', path)
        if m:
            bucket_name = m.group('bucket')
            key_name = m.group('key')
            key_name = re.sub(r'/+', '/', key_name)
            s3 = self.get_s3_connection()
            bucket = s3.get_bucket(bucket_name)
            if key_name:
                key = bucket.get_key(key_name)
            else:
                key = None
            return S3DavResource(self, bucket, key, key_name=key_name)
        assert False

class CacheDavServer(DavServer):
    def get_root(self):
        return os.path.abspath('cache')

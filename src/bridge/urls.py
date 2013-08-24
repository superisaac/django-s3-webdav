from django.conf.urls import patterns, include, url
from s3dav.server import CacheDavServer

# Uncomment the next two lines to enable the admin:
from django.contrib import admin
admin.autodiscover()

urlpatterns = patterns('',
    # Examples:
    url(r'^simple(?P<path>.*)$', 's3dav.views.webdav_export'),
    url(r'^cache(?P<path>.*)$', 's3dav.views.webdav_export', {'server_class': CacheDavServer}),
    url(r'^(?P<path>favicon.ico)', 'django.views.static.serve',
        {'document_root': 'static'}),
    url(r'^static/(?P<path>.*)$', 'django.views.static.serve',
        {'document_root': 'static'}),
    # Uncomment the next line to enable the admin:
    url(r'^admin/', include(admin.site.urls)),
    url(r'^(?P<bucket>[^/]+)/(?P<key>.*)$', 's3dav.views.export'),
    url(r'^(?P<bucket>[^/]+)$', 's3dav.views.notfound'),
    url(r'^$', 's3dav.views.export', {'bucket': '', 'key': ''}),
)

WebDAV adapter for Amazon's s3 storage service

Installation
============

Install django and other dependancies

```!bash
% easy_install -U django
% easy_install -U python-dateutil
% easy_install -U boto
```

Run
===========

* cd /path/to/django-s3-webdav/src
* mkdir -p cache  #
* edit bridge/settings.py to possibly fill AWS_HOST and AWS_PORT, you can also create a local_settings.py as the sibling of settings.py
* python manager.py syncdb  # Syronize the database, during this process you need to input infomation of admin users, such as username, email and password, which are usful at admin page /admin/
* start the server ```python manage.py runserver 0.0.0.0:8000```

Use WebDAV
===========

As for Mac OS X Users, open Finder, Click Go -> Connect Server ->
Input http://<host:port>/ and Press connect. You need to input the username and password which are aws access id and aws access secret.

If there is no odd things happen a explorer window appears with several folder corresponding to your buckets, good luck!

Tips
===========

Tied of input the human unreadable aws access id and secret? You are lucky to make a shortcuts. 

Visit http://<host:port>/admin/ input the admin username and password created during db synchronization. Add an user with password, then add a S3Account object. Link the user and fill access id and secret with it. You can type the username and password at the webdav prompt know!


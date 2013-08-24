from django.contrib import admin
from s3dav.models import S3Account

for cls in [S3Account]:
    admin.site.register(cls)

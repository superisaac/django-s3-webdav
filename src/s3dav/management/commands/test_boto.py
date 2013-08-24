import getpass
import time
from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from s3dav.server import connect_boto
import boto.s3.key

class Command(BaseCommand):
    def handle(self, *args, **kw):
        s3 = connect_boto(settings.AWS_TEST_ACCESS_KEY_ID,
                          settings.AWS_TEST_SECRET_ACCESS_KEY)

        oldtime = time.time()
        buckets = list(s3.get_all_buckets())
        print time.time() - oldtime
        for bucket in buckets:
            oldtime = time.time()
            keys = list(bucket.get_all_keys())
            print bucket.name, len(keys), time.time() - oldtime


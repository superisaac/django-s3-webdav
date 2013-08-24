# -*- coding: utf-8 -*-

from django.db import models
from django.contrib.auth.models import User

class S3Account(models.Model):
    user = models.ForeignKey(User, unique=True)
    aws_access_key = models.CharField(max_length=100, db_index=True)
    aws_secret = models.CharField(max_length=100)

    class Meta:
        verbose_name = u'S3帐户'
        verbose_name_plural = verbose_name

    def __unicode__(self):
        return self.user.username

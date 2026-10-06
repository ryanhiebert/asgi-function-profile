from django.conf import settings
from django.db import models


class Note(models.Model):
    text = models.CharField(max_length=200)


class PrivateNote(models.Model):
    owner = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    text = models.CharField(max_length=200)

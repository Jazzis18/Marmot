from django.db import models

# Create your models here.


class Author(models.Model):
    imya = models.CharField(max_length=200)
    vozrast = models.IntegerField(default=0)


class Publication(models.Model):
    zagolovok = models.CharField(max_length=200)
    text = models.TextField()
    author = models.ForeignKey(Author)

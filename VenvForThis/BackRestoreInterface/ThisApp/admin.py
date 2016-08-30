from django.contrib import admin
from ThisApp.models import *

# Register your models here.


class PublicationAdmin(admin.ModelAdmin):
    list_display = ('zagolovok', 'author')

admin.site.register(Publication, PublicationAdmin)

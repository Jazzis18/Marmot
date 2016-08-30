from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^$', views.index, name='index'),
    url(r'^restore/', views.restore, name='backup'),
    url(r'^remove/', views.remove_database, name='base'),
]

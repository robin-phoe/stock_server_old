from django.conf.urls import url
from django.urls import path
from . import views,testdb

urlpatterns = [
    # url(r'^$',views.hello),
    path('hello/', views.hello),
]
urlpatterns = [
    # url(r'^$',views.hello),
    path('', views.runoob),
    path('testdb/', testdb.testdb),
]
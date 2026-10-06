from django.urls import path

from . import views

urlpatterns = [
    path("", views.hello),
    path("echo/", views.echo),
    path("notes/", views.notes),
    path("stream/", views.stream),
    path("download/", views.download),
]

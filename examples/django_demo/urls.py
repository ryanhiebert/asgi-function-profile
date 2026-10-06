from django.contrib.auth.views import LoginView, LogoutView
from django.urls import path

from . import views

urlpatterns = [
    path("", views.hello),
    path("echo/", views.echo),
    path("notes/", views.notes),
    path("stream/", views.stream),
    path("download/", views.download),
    path("login/", LoginView.as_view(template_name="django_demo/login.html")),
    path("logout/", LogoutView.as_view(next_page="/login/")),
    path("account/", views.account),
    path("me/", views.me),
]

"""Small, local-only Django project for the function-profile experiment."""

import os
from pathlib import Path

SECRET_KEY = "local-function-profile-experiment"
DEBUG = False
ALLOWED_HOSTS = ["localhost", "127.0.0.1", "testserver"]
ROOT_URLCONF = "examples.django_demo.urls"
INSTALLED_APPS = [
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "examples.django_demo",
]
MIDDLEWARE = [
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
]
TEMPLATES = [{
    "BACKEND": "django.template.backends.django.DjangoTemplates",
    "APP_DIRS": True,
    "OPTIONS": {"context_processors": [
        "django.contrib.auth.context_processors.auth",
    ]},
}]
LOGIN_URL = "/login/"
LOGIN_REDIRECT_URL = "/account/"
# Exact browser origins, including port. Missing/opaque origins are rejected.
WEBSOCKET_ALLOWED_ORIGINS = ["http://localhost:8000", "http://127.0.0.1:8000"]
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": os.environ.get(
            "FUNCTION_PROFILE_DEMO_DATABASE",
            str(Path(__file__).with_name("demo.sqlite3")),
        ),
        "ATOMIC_REQUESTS": True,
    }
}
DEFAULT_AUTO_FIELD = "django.db.models.AutoField"

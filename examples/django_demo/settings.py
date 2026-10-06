"""Small, local-only Django project for the function-profile experiment."""

import os
from pathlib import Path

SECRET_KEY = "local-function-profile-experiment"
DEBUG = False
ALLOWED_HOSTS = ["localhost", "127.0.0.1", "testserver"]
ROOT_URLCONF = "examples.django_demo.urls"
INSTALLED_APPS = ["examples.django_demo"]
MIDDLEWARE = [
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
]
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

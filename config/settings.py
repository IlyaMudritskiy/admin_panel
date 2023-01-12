import os
from pathlib import Path

from dotenv import dotenv_values
from split_settings.tools import include

# Set the base directory of the project
BASE_DIR = Path(__file__).resolve().parent.parent

# Import env values for Django
django_env = dotenv_values(f"{BASE_DIR}/env_files/django_app.env")

# Import PostgreSQL env values for DATABASES
postgres_env = dotenv_values(f"{BASE_DIR}/env_files/postgres.env")

SECRET_KEY = django_env["DJANGO_KEY"]
DEBUG = django_env["DEBUG"]
ALLOWED_HOSTS = []

include(
    "components/installed_apps.py"
    "components/middleware.py"
    )

ROOT_URLCONF = "config.urls"

include("componenst/templates.py")

WSGI_APPLICATION = "config.wsgi.application"

DATABASES = {
    "default": {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': postgres_env['POSTGRES_DB'],
        'USER': postgres_env['DB_USER'],
        'PASSWORD': postgres_env['DB_PASSWORD'],
        'HOST': postgres_env['POSTGRES_HOST'],
        'PORT': int(postgres_env['POSTGRES_PORT']),
        'OPTIONS': {
            'options': '-c search_path=public,content'
        }
    }
}

include("components/auth_password_validators.py")

LANGUAGE_CODE = "en-us"

TIME_ZONE = "UTC"

USE_I18N = True

USE_TZ = True

STATIC_URL = "static/"

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

import os
import sys
from os.path import join
from distutils.util import strtobool
import dj_database_url
from configurations import Configuration
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

os.environ["GDAL_DATA"] = "/usr/share/gdal/"

class Common(Configuration):


    INSTALLED_APPS = [
        "django.contrib.admin",
        "django.contrib.auth",
        "django.contrib.contenttypes",
        "django.contrib.sessions",
        "django.contrib.messages",
        "django.contrib.gis",
        "django.contrib.staticfiles",


        # Third party apps
        "rest_framework",            # utilities for rest apis
        "rest_framework_gis",        # GIS rest framework
        "rest_framework.authtoken",  # token authentication
        "django_filters",            # for filtering rest endpoints
        "corsheaders",               # enabled cross domain CORS requests
        "treebeard",                 # efficient tree representation
        "django_json_widget",        # admin widget for JSONField
        'whitenoise.runserver_nostatic',

        "debug_toolbar",

        # Your apps
        "wazimap_ng.users",
        "wazimap_ng.datasets",
        "wazimap_ng.extensions",
        "wazimap_ng.points",
        "wazimap_ng.boundaries",

    ]

    # https://docs.djangoproject.com/en/2.0/topics/http/middleware/
    MIDDLEWARE = [
        "django.middleware.security.SecurityMiddleware",
        "corsheaders.middleware.CorsMiddleware",
        "django.middleware.cache.UpdateCacheMiddleware",
        "debug_toolbar.middleware.DebugToolbarMiddleware",
        "whitenoise.middleware.WhiteNoiseMiddleware",
        "django.contrib.sessions.middleware.SessionMiddleware",
        "django.middleware.common.CommonMiddleware",
        "django.middleware.csrf.CsrfViewMiddleware",
        "django.contrib.auth.middleware.AuthenticationMiddleware",
        "django.contrib.messages.middleware.MessageMiddleware",
        "django.middleware.clickjacking.XFrameOptionsMiddleware",
        "django.middleware.cache.FetchFromCacheMiddleware",
    ]

    ALLOWED_HOSTS = ["*"]
    ROOT_URLCONF = "wazimap_ng.urls"
    SECRET_KEY = os.getenv("DJANGO_SECRET_KEY")
    WSGI_APPLICATION = "wazimap_ng.wsgi.application"

    # Email
    EMAIL_BACKEND = "django.core.mail.backends.smtp.EmailBackend"

    ADMINS = (
        ("Author", "adi@openup.org.za"),
    )


    # Postgres
    DATABASES = {
        "default": dj_database_url.config(
            default=os.getenv("DATABASE_URL", "postgis://postgres:@postgres:5432/postgres"),
            conn_max_age=int(os.getenv("POSTGRES_CONN_MAX_AGE", 600))
        )
    }

    CACHES = {
        'default': {
            # 'BACKEND': 'django.core.cache.backends.db.DatabaseCache',
            # 'LOCATION': 'table_cache',
            'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        }
    }

    # General
    APPEND_SLASH = False
    TIME_ZONE = "UTC"
    LANGUAGE_CODE = "en-us"
    # If you set this to False, Django will make some optimizations so as not
    # to load the internationalization machinery.
    USE_I18N = False
    USE_L10N = True
    USE_TZ = True
    LOGIN_REDIRECT_URL = "/"

    # Static files (CSS, JavaScript, Images)
    # https://docs.djangoproject.com/en/2.0/howto/static-files/
    STATIC_ROOT = os.path.normpath(join(os.path.dirname(BASE_DIR), "static"))
    STATICFILES_DIRS = []
    STATIC_URL = "/static/"
    STATICFILES_FINDERS = (
        "django.contrib.staticfiles.finders.FileSystemFinder",
        "django.contrib.staticfiles.finders.AppDirectoriesFinder",
    )
    STATICFILES_STORAGE = 'whitenoise.storage.CompressedManifestStaticFilesStorage'

    # Media files
    MEDIA_ROOT = join(os.path.dirname(BASE_DIR), "media")
    MEDIA_URL = "/media/"

    TEMPLATES = [
        {
            "BACKEND": "django.template.backends.django.DjangoTemplates",
            "DIRS": STATICFILES_DIRS,
            "APP_DIRS": True,
            "OPTIONS": {
                "context_processors": [
                    "django.template.context_processors.debug",
                    "django.template.context_processors.request",
                    "django.contrib.auth.context_processors.auth",
                    "django.contrib.messages.context_processors.messages",
                ],
            },
        },
    ]

    # Set DEBUG to False as a default for safety
    # https://docs.djangoproject.com/en/dev/ref/settings/#debug
    DEBUG = strtobool(os.getenv("DJANGO_DEBUG", "no"))

    # Password Validation
    # https://docs.djangoproject.com/en/2.0/topics/auth/passwords/#module-django.contrib.auth.password_validation
    AUTH_PASSWORD_VALIDATORS = [
        {
            "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
        },
        {
            "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
        },
        {
            "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
        },
        {
            "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
        },
    ]

    # Logging
    LOGGING = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "django.server": {
                "()": "django.utils.log.ServerFormatter",
                "format": "[%(server_time)s] %(message)s",
            },
            "verbose": {
                "format": "%(levelname)s %(asctime)s %(module)s %(process)d %(thread)d %(message)s"
            },
            "simple": {
                "format": "%(levelname)s %(message)s"
            },
        },
        "filters": {
            "require_debug_true": {
                "()": "django.utils.log.RequireDebugTrue",
            },
        },
        "handlers": {
            "django.server": {
                "level": "INFO",
                "class": "logging.StreamHandler",
                "formatter": "django.server",
            },
            "console": {
                "level": "DEBUG",
                "class": "logging.StreamHandler",
                "formatter": "simple"
            },
            "mail_admins": {
                "level": "ERROR",
                "class": "django.utils.log.AdminEmailHandler"
            }
        },
        "loggers": {
            "django": {
                "handlers": ["console"],
                "propagate": True,
            },
            "django.server": {
                "handlers": ["django.server"],
                "level": "INFO",
                "propagate": False,
            },
            "django.request": {
                "handlers": ["mail_admins", "console"],
                "level": "ERROR",
                "propagate": False,
            },
            "django.db.backends": {
                "handlers": ["console"],
                "level": "INFO"
            },
        }
    }

    # Custom user app
    AUTH_USER_MODEL = "users.User"

    # Django Rest Framework
    REST_FRAMEWORK = {
        "DEFAULT_PAGINATION_CLASS": "rest_framework.pagination.PageNumberPagination",
        "PAGE_SIZE": int(os.getenv("DJANGO_PAGINATION_LIMIT", 10)),
        "DATETIME_FORMAT": "%Y-%m-%dT%H:%M:%S%z",
        "DEFAULT_RENDERER_CLASSES": (
            "rest_framework.renderers.JSONRenderer",
            "rest_framework.renderers.BrowsableAPIRenderer",
            "rest_framework_csv.renderers.PaginatedCSVRenderer",
        ),
    }

    CORS_ORIGIN_ALLOW_ALL = True


TESTING = "test" in sys.argv


if TESTING:
    PASSWORD_HASHERS = [
        'django.contrib.auth.hashers.MD5PasswordHasher',
    ]
    print('=========================')
    print('In TEST Mode - Disableling Migrations')
    print('=========================')

    class DisableMigrations(object):

        def __contains__(self, item):
            return True

        def __getitem__(self, item):
            return "notmigrations"

    MIGRATION_MODULES = DisableMigrations()

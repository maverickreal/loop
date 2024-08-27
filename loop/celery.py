from celery import Celery
from os import environ as os_environ

os_environ.setdefault('DJANGO_SETTINGS_MODULE', 'loop.settings')

app = Celery('loop')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()

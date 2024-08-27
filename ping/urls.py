from django.conf.urls import url, include

from api import ReportResource
from views import get_report

from tastypie.api import Api

v1_api = Api("v1")
v1_api.register(ReportResource())

url_patterns = [
    url(r"^api/", include(v1_api.urls)),
    url(r"^get_report/$", get_report),
]
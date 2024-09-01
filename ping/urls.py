from django.conf.urls import url, include

from api import ReportResource

urlpatterns = [
    url(r'^get_report/(?P<id>\d+)/?$', ReportResource().wrap_view('dispatch_detail')),
    url(r'^trigger_report/?$', ReportResource().wrap_view('trigger_report')),
    url(r'^rebuild_datastore/?$', ReportResource().wrap_view('rebuild_datastore')),
]
from datetime import datetime, timedelta

from models import Report
from choices import DATA_POLL_PERIOD

from tastypie.resources import ModelResource

from django.conf.urls import url

class ReportResource(ModelResource):
    class Meta:
        resource_name = "report"
        queryset = Report.objects.all()

    def prepend_urls(self):
        return [
            url(r"^%s/trigger_report%s" % (
                    self._meta.resource_name, trailing_slash()
                ), self.wrap_view('trigger_report'),
                name="api_trigger_report")
        ]

    def trigger_report(self, request, **kwargs):
        report = Report.objects.order_by("-last_updated").first()

        # If the difference between the current datetime and that of
        # the report is upto 45 minutes then return the report id:
        if report and (datetime.now() - report.last_updated
                       <= timedelta(minutes=DATA_POLL_PERIOD)):
            return self.create_response(request, report.id)

        report = Report.objects.create()
        return self.create_response(request, report.id)

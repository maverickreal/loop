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
        """
        Initiates the generation of a report object.
        """
        report = Report.objects.order_by("-last_updated").first()

        # Since data source updates roughly only once per hour,
        # if it's been less that DATA_POLL_PERIOD seconds since
        # the last update, don't update again. Helps save the planet.
        if report and (datetime.now() - report.last_updated
                       <= timedelta(minutes=DATA_POLL_PERIOD)):
            return self.create_response(request, report.id)

        report = Report.objects.create()
        return self.create_response(request, report.id)

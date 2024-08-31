from datetime import datetime, timedelta

from models import Report
from choices import DATA_POLL_PERIOD

from ping.tasks import import_all_data, populate_report_instance
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
                name="api_trigger_report"),
            url(r"^%s/rebuild_datastore%s" % (
                    self._meta.resource_name, trailing_slash()
                ), self.wrap_view('rebuild_datastore'),
                name="api_rebuild_datastore"),
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
        populate_report_instance.delay(report.id)

        return self.create_response(request, report.id)

    def rebuild_datastore(self, request, **kwargs):
        """
        Parses the data sources to replenish the DB tables.
        """

        import_all_data.delay()

        return self.create_response(request, "ok")

    rebuild_datastore.allowed_methods = ['put']

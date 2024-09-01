from datetime import datetime, timedelta
import os

from loop import settings
from models import Report
from choices import DATA_POLL_PERIOD
from tasks import import_all_data

from tastypie.resources import ModelResource

from django.utils import timezone
from django.conf.urls import url
from django.http import Http404, HttpResponse

class ReportResource(ModelResource):
    class Meta:
        resource_name = "report"
        queryset = Report.objects.all()

    def obj_get(self, bundle, **kwargs):
        report_id = kwargs.get('pk')

        try:
            report = Report.objects.get(id=report_id)
        except Report.DoesNotExist:
            raise Http404("Report not found")

        # Return CSV file along with status message
        return self._get_response_with_status_and_file(report)

    def _get_response_with_status_and_file(self, report):
        """
        Handles attaching the CSV file to the response.
        """

        if not report.file:
            return HttpResponse("Couldn't find any file corresponding to this report", status=404)

        if not os.path.exists(report.file.path):
            report.delete()
            return HttpResponse("Could'nt find any file corresponding to this report", status=404)

        # Prepare the CSV response
        with open(file_path, 'r') as file:
            response = HttpResponse(file.read(), content_type='text/csv')
            response['Content-Disposition'] = 'attachment; filename="%s"' % os.path.basename(report.file.path)
            return response

    def prepend_urls(self):
        return [
            url(r"^%s/trigger_report/?$" % (
                    self._meta.resource_name
                ), self.wrap_view('trigger_report'),
                name="api_trigger_report"),
            url(r"^%s/rebuild_datastore/?$" % (
                    self._meta.resource_name
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
        if report and (timezone.now() - report.last_updated
                       <= timedelta(minutes=DATA_POLL_PERIOD)):
            return self.create_response(request, report.id)

        report = Report.objects.create()

        return self.create_response(request, report.id)

    trigger_report.allowed_methods = ['get']

    def rebuild_datastore(self, request, **kwargs):
        """
        Parses the data sources to replenish the DB tables.
        """

        import_all_data.delay()

        return self.create_response(request, "ok")

    rebuild_datastore.allowed_methods = ['put']

from datetime import datetime, timedelta
import os

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

    def create_response(self, request, data, response_class=HttpResponse, **response_kwargs):
        """
        Handles attaching the CSV file to the response.
        """
        if type(data) == int:
            return super(ReportResource, self).create_response(request, data, response_class, **response_kwargs)

        with open(data.obj.file.path, 'r') as file:
            response = HttpResponse(file.read(), content_type='text/csv')
            response['Content-Disposition'] = 'attachment; filename="%s"' % os.path.basename(data.obj.file.path)
            return response

    def obj_get(self, bundle, **kwargs):
        try:
            report = Report.objects.get(id=kwargs.get('id'))

            if not report.status:
                return HttpResponse("Running", status=200)

            # Return CSV file along with status message
            return self._get_response_with_status_and_file(report)
        except Report.DoesNotExist:
            raise Http404("Report not found")

    def _get_response_with_status_and_file(self, report):
        """
        Handles attaching the CSV file to the response.
        """

        if not report.file:
            return HttpResponse("Couldn't find any file corresponding to this report", status=404)

        if not os.path.exists(report.file.path):
            report.delete()
            return HttpResponse("Could'nt find any file corresponding to this report", status=404)

        return report

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

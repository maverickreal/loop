from django.shortcuts import redirect
from django.http import Http404, HttpResponse

from models import Report

def get_report(request):
    report = Report.objects.filter(id=request.GET.get("id")).first()

    if not report:
        raise Http404("Report not found")

    if not report.status:
        return HttpResponse("Running", status=200)

    return redirect("/api/v1/report/%s" % report.id)

def trigger_report(request):
    return redirect("/api/v1/report/trigger_report")

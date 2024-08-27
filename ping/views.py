from django.shortcuts import redirect

def get_report(request):
    return redirect("/api/v1/report/")

from csv import writer as csv_writer
from tempfile import TemporaryDirectory

from django.db.models import Sum, Case, When, Value, IntegerField, F, Q
from django.db.models.functions import Extract

from celery import shared_task

from models import Poll, StoreBusinessHour
from choices import REPORT_GENERATION_RETRY_DELAY

def get_queryset_for_report():
    """
    A report consists of the uptime and downtime in the last hour, day, and week.
    """
    return (
        Poll.objects.annotate(day_of_week=Extract(F('timestamp'), 'dow'))
            .left_join(StoreBusinessHour, on={
                'store_id': F('store_id'), 
                'day_of_week': (Extract(F('timestamp'), 'dow') + 6) % 7
            }).annotate(
                uptime_week=Sum(Case(When(
                    Q(timestamp__range=[start_date_time_with_timezone_week, end_date_time_with_timezone_week]) &
                    Q(status='active'),
                    then=Value(1)
                    ), default=Value(0), output_field=IntegerField())),
                uptime_day=Sum(Case(When(
                    Q(timestamp__range=[start_date_time_with_timezone_day, end_date_time_with_timezone_day]) &
                    Q(status='active'),
                    then=Value(1)
                    ), default=Value(0), output_field=IntegerField())),
                uptime_hour=Sum(Case(When(
                    Q(timestamp__range=[start_date_time_with_timezone_hour, end_date_time_with_timezone_hour]) &
                    Q(status='active'),
                    then=Value(1)
                    ), default=Value(0), output_field=IntegerField())),
                downtime_week=Sum(Case(When(
                    Q(timestamp__range=[start_date_time_with_timezone_week, end_date_time_with_timezone_week]) &
                    Q(status='active'),
                    then=Value(0)
                    ), default=Value(1), output_field=IntegerField())),
                downtime_day=Sum(Case(When(
                    Q(timestamp__range=[start_date_time_with_timezone_day, end_date_time_with_timezone_day]) &
                    Q(status='active'),
                    then=Value(0)
                    ), default=Value(1), output_field=IntegerField())),
                downtime_hour=Sum(Case(When(
                    Q(timestamp__range=[start_date_time_with_timezone_hour, end_date_time_with_timezone_hour]) &
                    Q(status='active'),
                    then=Value(0)
                    ), default=Value(1), output_field=IntegerField()))
            ).filter(timestamp__range=[F('bh__start_time_local'), F('bh__end_time_local')])
    )

@shared_task(retry_jitter=True, ignore_result=True, retry_delay=REPORT_GENERATION_RETRY_DELAY, bind=True, max_retries=3)
def populate_report_instance(report):
    """
    Reads the poll logs from DB,
    generates a report,
    and saves it to a CSV file.
    """
    try:
        temp_dir = TemporaryDirectory()
        temp_file_path = temp_dir.name + "/report.csv"

        with open(temp_file_path, "w") as file:
            writer = csv_writer(file)
            writer.writerow(["store_id", "uptime_last_hour(in minutes)", "uptime_last_day(in hours)",
                                "update_last_week(in hours)", "downtime_last_hour(in minutes)",
                                "downtime_last_day(in hours)", "downtime_last_week(in hours)"])

            start_date_time_with_timezone_week = '2024-08-01 00:00:00'
            end_date_time_with_timezone_week = '2024-08-07 23:59:59'
            start_date_time_with_timezone_day = '2024-08-01 00:00:00'
            end_date_time_with_timezone_day = '2024-08-01 23:59:59'
            start_date_time_with_timezone_hour = '2024-08-01 12:00:00'
            end_date_time_with_timezone_hour = '2024-08-01 13:00:00'

            report_qs = get_queryset_for_report().\
                values("store_id", "uptime_hour", "uptime_day",
                       "uptime_week", "downtime_hour", "downtime_day",
                       "downtime_week")

            for store_datum in report_qs:
                writer.writerow(store_datum.values())

            report.refresh_from_db()
            report.file.save("report.csv", open(temp_file_path, 'rb'))
            report.status = True
            report.save()

        temp_dir.cleanup()
    except Exception as e:
        # must enable logging from hereon
        raise self.retry(exc=e)

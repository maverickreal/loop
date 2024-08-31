from csv import writer as csv_writer
import datetime

from django.db import transaction

from backports import tempfile
from celery import shared_task

import choices
from helpers import populate_objects, get_store_uptime_downtime
import models

@shared_task(retry_jitter=True, ignore_result=True, retry_delay=choices.REPORT_GENERATION_RETRY_DELAY, bind=True, max_retries=3)
def populate_report_instance(report):
    """
    Reads the poll logs from DB,
    generates a report,
    and saves it to a CSV file.
    """
    try:
        temp_dir = tempfile.TemporaryFile()
        temp_file_path = temp_dir.name + "/report.csv"

        with open(temp_file_path, "w") as file:
            writer = csv_writer(file)
            writer.writerow(["store_id", "uptime_last_hour(in minutes)", "uptime_last_day(in hours)",
                                "update_last_week(in hours)", "downtime_last_hour(in minutes)",
                                "downtime_last_day(in hours)", "downtime_last_week(in hours)"])

            report_qs = get_store_uptime_downtime()\
                .values("store_id", "uptime_hour", "uptime_day",
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

@shared_task(retry_jitter=True, ignore_result=True, retry_delay=choices.DB_IMPORT_RETRY_DELAY, bind=True, max_retries=3)
@transaction.atomic
def import_all_data(stores_file, business_hours_file, polls_file):
    """
    Populates the DB tables from CSV data sources.
    """
    populate_objects(stores_file, models.Store)
    populate_objects(business_hours_file, models.StoreBusinessHour)
    populate_objects(polls_file, models.Poll)

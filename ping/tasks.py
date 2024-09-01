from csv import writer as csv_writer
from datetime import datetime, timedelta

from backports import tempfile
from celery import shared_task

from django.db.models import Max

import choices
from helpers import fine_tune_aggregate, get_store_uptime_downtime, populate_objects, get_store_stats
import models

@shared_task(retry_jitter=True,
             ignore_result=True,
             retry_delay=choices.REPORT_GENERATION_RETRY_DELAY,
             max_retries=3,
             bind=True)
def populate_report_instance(self, report_id):
    """
    Reads the poll logs from DB,
    generates a report,
    and saves it to a CSV file.
    """

    try:
        temp_dir = tempfile.TemporaryDirectory()
        temp_file_path = temp_dir.name + "/report.csv"

        with open(temp_file_path, "w") as file:
            writer = csv_writer(file)
            writer.writerow(["store_id", "uptime_last_hour(in minutes)", "uptime_last_day(in hours)",
                                "update_last_week(in hours)", "downtime_last_hour(in minutes)",
                                "downtime_last_day(in hours)", "downtime_last_week(in hours)"])
            store_data = []
            max_timestamp = models.Poll.objects.aggregate(max_ts=Max('timestamp'))['max_ts']

            for store_id, poll_timestamp, status in get_store_stats(max_timestamp):
                store_id = int(store_id)
                poll_timestamp = datetime.fromtimestamp(poll_timestamp)
                status = status=='t'

                if not store_data or store_id==store_data[-1][0]:
                    store_data.append((store_id, poll_timestamp, status))
                else:
                    cur_row = get_store_uptime_downtime(store_data, poll_timestamp)
                    cur_row = fine_tune_aggregate(store_data, *cur_row)
                    cur_row = [store_data[-1][0]] + cur_row
                    writer.writerow(cur_row)
                    store_data = []

            if store_data:
                cur_row = get_store_uptime_downtime(store_data, poll_timestamp)
                cur_row = fine_tune_aggregate(store_data, *cur_row)
                cur_row = [store_data[-1][0]] + cur_row
                writer.writerow(cur_row)

            report = models.Report.objects.filter(id=report_id).first()
            report.file.save("report.csv", open(temp_file_path, 'rb'))
            report.status = True
            report.save()

        temp_dir.cleanup()
    except Exception as e:
        # TODO: must enable logging from hereon
        print("Error in populate_report_instance: %s" % e)
        raise self.retry(exc=e)

@shared_task(retry_jitter=True,
             ignore_result=True,
             retry_delay=choices.DB_IMPORT_RETRY_DELAY,
             max_retries=3,
             bind=True)
def import_all_data(self):
    """
    Populates the DB tables from CSV data sources.
    """
    try:
        print("Kindly be patient. This may take upto several minutes.")
        populate_objects(models.Store)
        print("Done populating stores.")
        populate_objects(models.StoreBusinessHour)
        print("Done populating store business hours.")
        populate_objects(models.Poll)
        print("Done populating store polls.")
    except Exception as e:
        # TODO: must enable logging from hereon
        print("Error in import_all_data: %s" % e)
        raise self.retry(exc=e)

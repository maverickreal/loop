from pathlib import Path
from datetime import time as datetime_time

from django.db import models
from django.core.validators import FileExtensionValidator

from choices import REPORTS_DIR
from tasks import populate_report_instance

class Poll(models.Model):
    """
    The status of a store at a given datetime.
    """
    status = models.BooleanField(null=False)
    store_id = models.BigIntegerField(null=False)
    timestamp = models.DateTimeField(null=False)


class StoreTimezone(models.Model):
    """
    The timezone of a store.
    """
    store_id = models.BigIntegerField(null=False)
    timezone = models.CharField(max_length=100, null=False, default="America/Chicago")


class StoreBusinessHour(models.Model):
    """
    The business hours of a store.
    """
    store_id = models.BigIntegerField(null=False)

    MONDAY = 0
    TUESDAY = 1
    WEDNESDAY = 2
    THURSDAY = 3
    FRIDAY = 4
    SATURDAY = 5
    SUNDAY = 6

    DAYS_OF_WEEK = (
        (MONDAY, "Monday"),
        (TUESDAY, "Tuesday"),
        (WEDNESDAY, "Wednesday"),
        (THURSDAY, "Thursday"),
        (FRIDAY, "Friday"),
        (SATURDAY, "Saturday"),
        (SUNDAY, "Sunday")
    )

    day_of_week = models.SmallIntegerField(choices=DAYS_OF_WEEK, null=False)
    start_time_local = models.TimeField(null=False, default=datetime_time(0, 0))
    end_time_local = models.TimeField(null=False, default=datetime_time(23, 59))


class Report(models.Model):
    """
    Information about the uptime and downtime over
    a span of an hour, day, and week. The actual
    data will be stored in CSV files.
    """
    def __generate_file_path(self, filename):
        return "{}/{}{}".format(REPORTS_DIR, self.id, Path(filename).suffix)

    last_updated = models.DateTimeField(auto_now_add=True, null=False)
    file = models.FileField(
        upload_to=__generate_file_path,
        null=False, unique=True,
        validators=[FileExtensionValidator(["csv"])])
    status = models.BooleanField(null=False, default=False)

    def save(self, *args, **kwargs):
        # Generate data and store it in a CSV file.
        populate_report_instance(self)
        super(Report, self).save(*args, **kwargs)

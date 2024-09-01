from datetime import time as datetime_time

from django.db import models
from django.core.validators import FileExtensionValidator

from pathlib2 import Path

from ping.helpers import generate_file_path
from tasks import populate_report_instance

class BaseDataSourceModel(models.Model):
    """
    Base model class for all model classes that
    are populated through data sources.
    """

    @classmethod
    def data_source_filename(cls, filetype):
        return "{}.{}".format(cls._meta.db_table, filetype)

    def data_destination_filename(self, filename):
        return generate_file_path(self, filename)

    class Meta:
        abstract = True


class Store(BaseDataSourceModel):
    """
    The timezone of a store.
    """

    id = models.BigIntegerField(primary_key=True)
    timezone = models.CharField(max_length=100,
                                null=False,
                                default="America/Chicago")

    class Meta:
        indexes = [
            models.Index(fields=['timezone']),
        ]


class Poll(BaseDataSourceModel):
    """
    The status of a store at a given datetime.
    """
    status = models.BooleanField(null=False)
    store = models.ForeignKey(Store, on_delete=models.PROTECT,
                              null=False,
                              related_name="polls")
    timestamp = models.DateTimeField(null=False)

    class Meta:
        indexes = [
            models.Index(fields=['timestamp']),
            models.Index(fields=['store', 'timestamp']),
        ]


class StoreBusinessHour(BaseDataSourceModel):
    """
    The business hours of a store.
    """
    store = models.ForeignKey(Store,
                              on_delete=models.PROTECT,
                              null=False,
                              related_name="business_hours")

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

    day_of_week = models.SmallIntegerField(choices=DAYS_OF_WEEK,
                                           null=False)
    start_time_local = models.TimeField(null=False,
                                        default=datetime_time(0, 0))
    end_time_local = models.TimeField(null=False,
                                      default=datetime_time(23, 59))

    class Meta:
        indexes = [
            models.Index(fields=['store', 'day_of_week']),
        ]


class Report(BaseDataSourceModel):
    """
    Information about the uptime and downtime over
    a span of an hour, day, and week. The actual
    data will be stored in CSV files.
    """

    last_updated = models.DateTimeField(auto_now_add=True,
                                        null=False)
    file = models.FileField(
        upload_to=generate_file_path,
        null=False, unique=True,
        validators=[FileExtensionValidator(["csv"])])
    status = models.BooleanField(null=False,
                                 default=False)

    def save(self, *args, **kwargs):
        # Generate data and store it in a CSV file.
        super(Report, self).save(*args, **kwargs)
        populate_report_instance.delay(self.id)

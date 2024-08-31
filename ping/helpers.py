import csv
from datetime import datetime

from pathlib2 import Path

from django.db.models import Sum, Case, When, IntegerField, F, Value
from django.db.models.functions import Extract
from django.utils import timezone

from ping.choices import DATA_SOURCE_PARSE_CHUNK_SIZE, REPORTS_DIR
import models

def generate_file_path(self, filename):
    return "{}/{}{}".format(REPORTS_DIR,
                            self.id,
                            Path(filename).suffix)

def get_store_uptime_downtime():
    # Calculate time ranges
    now = models.Poll.objects.order_by("-timestamp").first().timestamp
    start_week = now - timezone.timedelta(days=7)
    start_day = now - timezone.timedelta(days=1)
    start_hour = now - timezone.timedelta(hours=1)

    return models.Store.objects.annotate(
        uptime_week=Sum(Case(
            When(polls__timestamp__range=(start_week, now),
                 polls__status=True,
                 then=1),
            default=0,
            output_field=IntegerField()
        )),
        uptime_day=Sum(Case(
            When(polls__timestamp__range=(start_day, now),
                 polls__status=True,
                 then=1),
            default=0,
            output_field=IntegerField()
        )),
        uptime_hour=Sum(Case(
            When(polls__timestamp__range=(start_hour, now),
                 polls__status=True,
                 then=1),
            default=0,
            output_field=IntegerField()
        )),
        downtime_week=Sum(Case(
            When(polls__timestamp__range=(start_week, now),
                 polls__status=False,
                 then=1),
            default=0,
            output_field=IntegerField()
        )),
        downtime_day=Sum(Case(
            When(polls__timestamp__range=(start_day, now),
                 polls__status=False,
                 then=1),
            default=0,
            output_field=IntegerField()
        )),
        downtime_hour=Sum(Case(
            When(polls__timestamp__range=(start_hour, now),
                 polls__status=False,
                 then=1),
            default=0,
            output_field=IntegerField()
        ))
    ).filter(
        business_hours__day_of_week=((
            Extract('polls__timestamp', 'dow') + Value(6)
        ) % Value(7)),
        polls__timestamp__range=(F('business_hours__start_time_local'),
                                 F('business_hours__end_time_local'))
    ).values('id', 'uptime_week', 'uptime_day', 'uptime_hour',
             'downtime_week', 'downtime_day', 'downtime_hour')

def populate_objects(file_path, ModelClass):
    """
    Populates the DB table for model from
    CSV data source at file_path.
    """

    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        instances = []

        for row in reader:
            attrs = {}
            if ModelClass == models.Store:
                attrs = {
                    "id": row['store_id'],
                    "timezone": row['timezone_str']
                    }
            elif ModelClass == models.Poll:
                attrs = {
                    "store_id": row['store_id'],
                    "status": row['status'] == 'active',
                    "timestamp": timezone.make_aware(datetime.strptime(row['timestamp_utc'], r'%Y-%m-%d %H:%M:%S.%f %Z'))
                    }
            elif ModelClass ==  models.StoreBusinessHour:
                attrs = {
                    "store_id": row['store_id'],
                    "day_of_week": int(row['day']),
                    "start_time_local": row['start_time_local'],
                    "end_time_local": row['end_time_local']
                    }

            instances.append(ModelClass(**attrs))

            if len(instances) == DATA_SOURCE_PARSE_CHUNK_SIZE:
                ModelClass.objects.bulk_create(instances, ignore_conflicts=True)
                instances = []

        if instances:
            ModelClass.objects.bulk_create(instances, ignore_conflicts=True)

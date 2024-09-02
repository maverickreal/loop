import csv
from datetime import datetime
import os

from pathlib2 import Path

from django.db.models.functions import Extract
from django.utils import timezone
from django.db.models import Max, F, ExpressionWrapper, DateTimeField, IntegerField
from django.db.models.functions import Extract
from datetime import timedelta
from django.db import connection

from ping.choices import DATA_ROOT, DATA_SOURCE_PARSE_CHUNK_SIZE, DATA_SOURCES, REPORTS_DIR
import models

def loggit(*args, **kwargs):
    try:
        # TODO: Enable logging
        with open(os.path.join(DATA_ROOT, "log.log"), "a") as file:
            for arg in args:
                print(arg)
                file.write(str(arg) + "\n")
    except Exception as err:
        loggit("Error logging: {}".format(err))

def generate_file_path(obj, filename):
    """
    Generates a file path for a data
    Source based model instance.
    """

    return "{}/{}{}".format(REPORTS_DIR,
                            obj.id,
                            Path(filename).suffix)

def get_store_stats(max_timestamp):
    # Get the maximum poll timestamp.
    # max_poll_time = models.Poll.objects.aggregate(max_ts=Max('timestamp'))['max_ts']
    # qs = (
    #     models.Poll.objects.select_related('store').filter(
    #         timestamp__gte=max_poll_time - timedelta(weeks=1),
    #         timestamp__lte=max_poll_time
    #     ).annotate(
    #         local_time=ExpressionWrapper(
    #             F('timestamp') + F('store__timezone'),
    #             output_field=DateTimeField()
    #         ),
    #         day_of_week=ExpressionWrapper(
    #             (Extract('timestamp', 'dow') + 6) % 7,
    #             output_field=IntegerField()
    #         )
    #     ).filter(
    #         store__business_hours__day_of_week=F('day_of_week'),
    #         local_time__time__gte=F('store__business_hours__start_time_local'),
    #         local_time__time__lte=F('store__business_hours__end_time_local')
    #     ).order_by('store_id', 'timestamp')
    # )
    with connection.cursor() as cursor:
        query = """
                SELECT store.id, poll.timestamp, poll.status
                FROM
                    ping_poll AS poll
                    INNER JOIN ping_store AS store ON poll.store_id = store.id
                    LEFT JOIN ping_storebusinesshour AS bh ON (
                        store.id = bh.store_id
                        AND bh.day_of_week = (
                            (
                                CAST(
                                    EXTRACT(
                                        DOW
                                        FROM poll.timestamp
                                    ) AS INT
                                ) + 6
                            ) % 7
                        )
                    )
                WHERE
                    ((
                        poll.timestamp AT TIME ZONE 'UTC' AT TIME ZONE store.timezone
                    )::time BETWEEN COALESCE(bh.start_time_local, '00:00:00'::time) AND
                    COALESCE(bh.end_time_local, '23:59:59'::time))
                    AND (poll.timestamp BETWEEN (CAST('{max_ts}' AS TIMESTAMPTZ)
                        - INTERVAL '1 week') AND CAST('{max_ts}' AS TIMESTAMPTZ))
                ORDER BY store.id, poll.timestamp;
                """.format(max_ts=max_timestamp)

        cursor.execute(query)

        for result in iter(cursor.fetchone, None):
            yield result

def get_datetime_from_str(str):
    """
    Converts a string into a datetime object.
    """
    try:
        date_time = timezone.make_aware(datetime.strptime(str, r'%Y-%m-%d %H:%M:%S.%f %Z'))
    except Exception as err:
        # TODO: Enable logging
        date_time = timezone.make_aware(datetime.strptime(str, r'%Y-%m-%d %H:%M:%S %Z'))

    return date_time

def populate_objects(ModelClass):
    """
    Populates the DB table for model from
    data source.
     #TODO: Currently only sourcing from CSV
     files enabled. Enable support fo other
     file types, and other data source types.
    """

    file_path = os.path.join(DATA_SOURCES["CSV"], ModelClass.data_source_filename("csv"))

    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        instances = []

        for row in reader:
            attrs = {}

            # Adds missing store references.
            if ModelClass != models.Store:
                models.Store.objects.get_or_create(id=row['store_id'])

            if ModelClass == models.Store:
                attrs = {
                    "id": row['store_id'],
                    "timezone": row['timezone_str']
                    }
            elif ModelClass == models.Poll:
                attrs = {
                    "store_id": row['store_id'],
                    "status": row['status'] == 'active',
                    "timestamp": get_datetime_from_str(row['timestamp_utc'])
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
                # Already atomic.
                ModelClass.objects.bulk_create(instances)
                instances = []

        if instances:
            ModelClass.objects.bulk_create(instances)

def fine_tune_aggregate(store_data,
                        uptime_week,
                        uptime_day,
                        uptime_hour,
                        downtime_week,
                        downtime_day,
                        downtime_hour):
    """
    To deal with unexpected corrupt poll logs.
    """

    uptime_week = min(int(uptime_week), 7*24)
    uptime_day = min(int(uptime_day), 24)
    uptime_hour = min(int(uptime_hour), 1) * 60

    downtime_week = min(int(downtime_week), 7*24)
    downtime_day = min(int(downtime_day), 24)
    downtime_hour = min(int(downtime_hour), 1) * 60

    # if no poll log was available for the last hour, then to compute
    # its uptime and downtime, we need to use the last poll log:
    if not (uptime_hour or downtime_hour):
        _, _, last_status = store_data[-1]

        if last_status:
            uptime_hour = 60
        else:
            downtime_hour = 60
    elif (uptime_hour + downtime_hour) > 60:
        if uptime_hour > downtime_hour:
            uptime_hour -= (uptime_hour + downtime_hour) - 60
        else:
            downtime_hour -= (uptime_hour + downtime_hour) - 60
    elif uptime_hour > downtime_hour:
        uptime_hour += 60 - (uptime_hour + downtime_hour)
    else:
        downtime_hour += 60 - (uptime_hour + downtime_hour)

    if (uptime_day + downtime_day) > 24:
        if uptime_day > downtime_day:
            uptime_day -= (uptime_day + downtime_day) - 24
        else:
            downtime_day -= (uptime_day + downtime_day) - 24
    elif uptime_day > downtime_day:
        uptime_day += 24 - (uptime_day + downtime_day)
    else:
        downtime_day += 24 - (uptime_day + downtime_day)

    if (uptime_week + downtime_week) > 7*24:
        if uptime_week > downtime_week:
            uptime_week -= (uptime_week + downtime_week) - 7*24
        else:
            downtime_week -= (uptime_week + downtime_week) - 7*24
    elif uptime_week > downtime_week:
        uptime_week += 7*24 - (uptime_week + downtime_week)
    else:
        downtime_week += 7*24 - (uptime_week + downtime_week)

    return [uptime_hour,
            uptime_day,
            uptime_week,
            downtime_hour,
            downtime_day,
            downtime_week]

def get_store_uptime_downtime(store_data, cur_ts):
    uptime_week = 0
    uptime_day = 0
    uptime_hour = 0

    downtime_week = 0
    downtime_day = 0
    downtime_hour = 0

    for _, poll_timestamp, status in store_data:
        within_hour = (cur_ts - poll_timestamp) <= timedelta(hours=1)
        within_day = (cur_ts - poll_timestamp) <= timedelta(days=1)

        if status:
            if within_hour:
                uptime_hour = 1
            if within_day:
                uptime_day += 1
            uptime_week += 1
        else:
            if within_hour:
                downtime_hour = 1
            if within_day:
                downtime_day += 1
            downtime_week += 1

    return [uptime_week,
            uptime_day,
            uptime_hour,
            downtime_week,
            downtime_day,
            downtime_hour]

from supporting import aws
from datetime import datetime
import logging
from database.db import Connection
import os
import math
import json
import uuid


class CorrelationIdFilter(logging.Filter):
    def __init__(self):
        super().__init__()
        # Generate a new correlation ID
        self.correlation_id = str(uuid.uuid4())

    def filter(self, record):
        # Add correlation ID to the log record
        record.correlation_id = self.correlation_id
        return True


# Logging formatter that includes the correlation ID
formatter = logging.Formatter('[%(levelname)s] [%(asctime)s] [Correlation ID: %(correlation_id)s] %(message)s')

# Set up the root logger
log = logging.getLogger()
log.setLevel("INFO")
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)

# Remove existing handlers
for handler in log.handlers:
    log.removeHandler(handler)

# Add a new handler with the custom formatter
handler = logging.StreamHandler()
handler.setFormatter(formatter)
log.addHandler(handler)

# Add the CorrelationIdFilter to the logger
correlation_filter = CorrelationIdFilter()
log.addFilter(correlation_filter)


def speed_to_pace(speed):
    seconds = 3600 / speed
    minutes = math.floor(seconds / 60)
    remain = round(seconds - round(minutes * 60, 0))
    if remain < 10:
        remain = "0"+str(remain)
    return f"{minutes}:{remain}"


def total_time(time):
    if time >= 86400:
        days = math.floor(time / 86400)

        time = time - days*86400
        if days > 1:
            return_time = f"{days} dagen"
        else:
            return_time = f"{days} dag"
    else:
        return_time = "0 dagen"
    if time >= 3600:
        hours = math.floor(time / 3600)
        time = time - hours*3600
        if hours > 1:
            return_time += f", {hours} uren"
        else:
            return_time += f", {hours} uur"
    else:
        return_time += ", 0 uur"

    if time >= 60:
        minutes = math.floor(time / 60)
        if minutes > 1:
            return_time += f", {minutes} minuten"
        else:
            return_time += f", {minutes} minuut"

        time = int(time - minutes*60)
    else:
        return_time += ", 0 minuten"

    if time > 0:
        if time > 1:
            return_time += f", {time} seconden"
        else:
            return_time += f", {time} seconde"
    else:
        return_time += ", 0 seconden"
    return return_time.strip()


def lambda_handler(event, context):
    database_id = os.getenv('DATABASE_ID')
    database_settings = aws.dynamodb_query(table='database_settings', id=database_id)
    db_host = database_settings[0]['host']
    db_user = database_settings[0]['user']
    db_password = database_settings[0]['password']
    db_port = database_settings[0]['port']
    db = Connection(user=db_user, password=db_password, host=db_host, port=db_port, charset="utf8mb4")
    log.info('START')
    current_year = datetime.now().year

    data = {}
    years = []
    wind_direction_array = {}
    wind_speed_array = {}
    temp_array = {}
    humidity_array = {}

    query = f"SELECT DISTINCT(YEAR(start_date_local)) FROM activity WHERE YEAR(start_date_local) = {current_year} order by start_date_local"
    result_year = db.get_specific(custom=query)
    for item in result_year:
        data[item[0]] = {}
        wind_direction_array[item[0]] = 0
        wind_direction_array[item[0]] = 0
        wind_speed_array[item[0]] = 0
        temp_array[item[0]] = 0
        humidity_array[item[0]] = 0
        years.append(item[0])
    del result_year

    query = F"SELECT weather_meteo.temp, weather_meteo.wind_direction, weather_meteo.wind_speed, weather_meteo.humidity, year(activity.start_date_local), activity.id  " \
            F"FROM weather_meteo " \
            F"INNER JOIN activity ON activity.id = weather_meteo.activity_id WHERE YEAR(start_date_local) = {current_year} ORDER BY year(activity.start_date_local)"
    result_weather = db.get_specific(custom=query)
    total_wind_direction = 0
    total_wind_speed = 0
    total_temp = 0
    total_humidity = 0
    count_wind_direction = 0
    count_wind_speed = 0
    count_temp = 0
    count_humidity = 0
    for item in result_weather:
        try:
            wind_direction = item[1].split(', ')
            for wind in wind_direction:
                total_wind_direction += float(wind)
                count_wind_direction += 1
            del wind_direction
            wind_speed = item[2].split(', ')
            for wind in wind_speed:
                total_wind_speed += float(wind)
                count_wind_speed += 1
            del wind_speed
            temp = item[0].split(', ')
            for t in temp:
                total_temp += float(t)
                count_temp += 1

            humidity = item[3].split(', ')
            del temp
            for h in humidity:
                total_humidity += float(h)
                count_humidity += 1
            del humidity
            wind_direction_array[item[4]] = round(total_wind_direction / count_wind_direction, 1)
            wind_speed_array[item[4]] = round(total_wind_speed / count_wind_speed, 1)
            temp_array[item[4]] = round(total_temp / count_temp, 1)
            humidity_array[item[4]] = round(total_humidity / count_humidity, 1)

        except Exception as e:
            log.info(e)
            log.info(item)

            exit()
    del result_weather

    for year in years:
        data[year]["average_wind_direction"] = wind_direction_array[year]
        data[year]["average_wind_speed"] = wind_direction_array[year]
        data[year]["average_temp"] = temp_array[year]
        data[year]["average_humidity"] = humidity_array[year]

    query = f"SELECT YEAR(start_date_local), sum(distance), sum(moving_time), sum(elapsed_time), avg(average_heartrate) FROM activity WHERE sport_type = 'Run' and distance <> 0 AND YEAR(start_date_local) = {current_year} group by YEAR(start_date_local) order by YEAR(start_date_local) desc "
    result_activity = db.get_specific(custom=query)

    for item in result_activity:
        year = item[0]
        average_speed = round(item[1] / item[2] * 3.6, 2)
        average_pace = speed_to_pace(average_speed)
        elapsed_time = total_time(item[3])
        moving_time = total_time(item[2])
        average_elapsed_speed = round(item[1] / item[3] * 3.6, 2)
        average_elapsed_pace = speed_to_pace(average_elapsed_speed)

        average_hr = item[4]
        if average_hr is None:
            average_hr = "-"
        else:
            average_hr = round(average_hr, 1)
        total_distance = f"{round(item[1]/1000, 2)} km"
        data[year]["total_distance"] = total_distance
        data[year]["average_pace"] = average_pace
        data[year]["average_elapsed_pace"] = average_elapsed_pace
        data[year]["elapsed_time"] = elapsed_time
        data[year]["moving_time"] = moving_time
        data[year]["average_hr"] = average_hr
    del result_activity

    for item in data:
        try:
            json_data = {
                'value': json.dumps(data[item])
            }
            db.update(table='stats', json_data=json_data, record_id=item, mode='single', unique_column='year')

        except:
            json_data = {
                'year': item,
                'value': json.dumps(data[item])
            }
            db.insert(table='stats', json_data=json_data)


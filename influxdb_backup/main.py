"""
This script stores sensor data of the last days that is in an influxdb to csv's.
It should be run periodically, like daily.
"""
import os.path
from datetime import datetime, timedelta
from typing import List, Dict

import pandas as pd
from dotenv import load_dotenv
from influxdb import InfluxDBClient


def get_all_measurement_names(client: InfluxDBClient) -> List[str]:
    result = client.query("SHOW MEASUREMENTS")
    measurements = list(result.get_points())

    return [m['name'] for m in measurements]


def get_df_for_measurement(client: InfluxDBClient, query: str) -> pd.DataFrame:
    result = client.query(query)
    points = list(result.get_points())
    data = []
    for point in points:
        data.append(point)

    df = pd.DataFrame(data=data)
    if not df.empty:
        df["time"] = pd.to_datetime(df["time"], format='%Y-%m-%dT%H:%M:%S.%fZ', utc=True)
        df["time"] = df["time"].dt.tz_convert('Europe/Amsterdam')

    return df


def get_all_measurements(client: InfluxDBClient, from_date: datetime, to_date: datetime) -> Dict[str, pd.DataFrame]:
    all_measurements = {}
    for measurement in get_all_measurement_names(client):
        query = f'''
        SELECT * FROM "{measurement}" 
        WHERE time >= '{from_date.strftime('%Y-%m-%dT%H:%M:%SZ')}' 
        AND time <=  '{to_date.strftime('%Y-%m-%dT%H:%M:%SZ')}'
        tz('Europe/Amsterdam')
        '''

        all_measurements[measurement] = get_df_for_measurement(client, query)

    return all_measurements


def get_measurements_of_one_day(client: InfluxDBClient, day: datetime) -> Dict[str, pd.DataFrame]:
    midnight_of_day = day.replace(hour=0, minute=0, second=0, microsecond=0)

    return get_all_measurements(
        client=client,
        from_date=midnight_of_day,
        to_date=midnight_of_day + timedelta(days=1)
    )


def backup_one_day_if_not_exists(client: InfluxDBClient, base_folder: str, day: datetime) -> None:
    backup_folder = os.path.join(base_folder, str(day.year), day.strftime('%Y-%m-%d'))
    if os.path.exists(backup_folder):
        print(f"Backup folder already exists: {backup_folder}")
        return

    os.makedirs(backup_folder, exist_ok=False)

    for name, measurements_df in get_measurements_of_one_day(client=client, day=day).items():
        measurements_df.to_csv(os.path.join(backup_folder, f"{name}.csv"), )


def main():
    load_dotenv()

    client = InfluxDBClient(
        host=os.getenv('host'),
        port=os.getenv('port'),
        username=os.getenv('username'),
        password=os.getenv('password'),
        database=os.getenv('database'),
    )

    base_folder = os.path.join("C:/", "temp", "influx")
    os.makedirs(base_folder, exist_ok=True)

    # Do not backup anything from today, because folders will not be overwritten and today is not finished yet
    yesterday = datetime.now() - timedelta(days=1)
    for i in range(7):
        day = yesterday - timedelta(days=i)
        print(f"Making backup of {day}")

        backup_one_day_if_not_exists(client, base_folder, day)


if __name__ == '__main__':
    main()

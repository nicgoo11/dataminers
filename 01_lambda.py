import json
import boto3
import requests
from datetime import datetime, timedelta

S3_BUCKET_NAME = 'datawarehousingproject1'
S3_FOLDER = 'non_motorized_traffic/'
BASE_API_URL = 'https://data.bs.ch/api/explore/v2.1/catalog/datasets/100013/records'
DATE_FILE_NAME = 'current_date.json'

def read_current_date(s3_client):
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=DATE_FILE_NAME)
        date_data = json.loads(obj['Body'].read())
        return datetime.strptime(date_data['current_date'], '%Y-%m-%d')
    except Exception:
        # Starten Sie ab dem 01.01.2021, wenn keine Datei vorhanden ist
        return datetime(2021, 1, 1)

def write_current_date(s3_client, current_date):
    date_data = {'current_date': current_date.strftime('%Y-%m-%d')}
    s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=DATE_FILE_NAME, Body=json.dumps(date_data))

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    current_date = read_current_date(s3_client)
    offset = 0
    all_data = []

    while True:
        api_url = f"{BASE_API_URL}?limit=100&offset={offset}&refine=datetimeto:%22{current_date.year}%22&refine=month:%22{current_date.month}%22&refine=day:%22{current_date.day}%22"
        response = requests.get(api_url)
        data = response.json()

        if not data['results']:
            break

        all_data.extend(data['results'])
        offset += 100

    if all_data:
        file_name = f"{S3_FOLDER}{current_date.strftime('%Y-%m-%d')}.json"
        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=file_name, Body=json.dumps(all_data))

    next_date = current_date + timedelta(days=1)
    write_current_date(s3_client, next_date)

    # Prüfen, ob das heutige Datum erreicht ist
    is_today = current_date.date() >= datetime.now().date()

    return {
        'statusCode': 200,
        'body': json.dumps('Data retrieval and upload to S3 completed for date: ' + current_date.strftime('%Y-%m-%d')),
        'isToday': is_today  # Neues Feld, das angibt, ob das heutige Datum erreicht wurde
    }
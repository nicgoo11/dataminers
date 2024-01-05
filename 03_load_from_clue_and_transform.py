import boto3
import pandas as pd
import time

#AWS-Access
aws_access_key_id = 'yyy'
aws_secret_access_key = 'xxx'
aws_session_token = 'qqq'
region_name = 'us-east-1'

session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token
)

# Initialize Athena
athena = session.client('athena')

# List of databases
databases = [
    'speed_measurements',
    'accidents',
    'weather',
    'motorized_traffic',
    'non_motorized_traffic',
    'public_holidays'
]

# Define output location for query results in S3
s3_output = 's3://your-athena-query-results-bucket/folder/'

# Initialize a dictionary to hold the DataFrames
dfs = {}

# Loop over each database and run the query on the respective table
for database_name in databases:
    print(f"Querying database: {database_name}")

    table_name = database_name

    # Define SQL query
    query = f'SELECT * FROM {table_name}'

    # Execute the Athena query
    query_id = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database_name
        },
        ResultConfiguration={
            'OutputLocation': s3_output,
        }
    )['QueryExecutionId']

    # Implement a waiting mechanism (polling)
    while True:
        # Check query execution status
        status = athena.get_query_execution(QueryExecutionId=query_id)['QueryExecution']['Status']['State']

        if status == 'SUCCEEDED':
            print(f"Query SUCCEEDED for database {database_name}")
            break
        elif status in ['FAILED', 'CANCELLED']:
            raise Exception(f'Query {status} for database {database_name}')
        else:
            time.sleep(5)

    # Assuming the query completed, construct the result file path
    result_file = f'{query_id}.csv'

    # Define the local filename to save the result
    local_filename = f'{database_name}_result.csv'

    # Download the result file
    s3 = session.client('s3')
    s3.download_file('your-athena-query-results-bucket', f'folder/{result_file}', local_filename)

    # Read the
    df = pd.read_csv(local_filename)

    # Store the DataFrame
    dfs[f'{database_name}_df'] = df

    print(f"Data for database {database_name} written to {local_filename}")


# Define the start and end periods
start_period = pd.Period('2021-01', freq='M')
end_period = pd.Period('2023-12', freq='M')

# Process and filter Speed Measurements
speed_measurements_df['Month'] = pd.to_datetime(speed_measurements_df['Startzeit'], utc=True).dt.to_period('M')
speed_measurements_df = speed_measurements_df[(speed_measurements_df['Month'] >= start_period) & (speed_measurements_df['Month'] <= end_period)]
monthly_measurements = speed_measurements_df.groupby('Month').size().rename('Speed_Measurements')

# Process and filter Traffic Accidents
accidents_df['Month'] = pd.to_datetime(accidents_df['Unfalldatum'], utc=True).dt.to_period('M')
accidents_df = accidents_df[(accidents_df['Month'] >= start_period) & (accidents_df['Month'] <= end_period)]
monthly_accidents = accidents_df.groupby('Month').size().rename('Traffic_Accidents')

# Process and filter Weather Data
weather_df['Month'] = pd.to_datetime(weather_df['time']).dt.to_period('M')
weather_df = weather_df[(weather_df['Month'] >= start_period) & (weather_df['Month'] <= end_period)]
weather_monthly = weather_df.groupby('Month').agg({'temperature_2m': 'mean', 'rain': 'sum'}).rename(columns={'temperature_2m': 'Temperature', 'rain': 'Rain'})

# Process and filter Motorized Traffic
motorized_traffic_df['Month'] = pd.to_datetime(motorized_traffic_df['DateTimeFrom'], utc=True).dt.tz_convert(None).dt.to_period('M')
motorized_traffic_df = motorized_traffic_df[(motorized_traffic_df['Month'] >= start_period) & (motorized_traffic_df['Month'] <= end_period)]
monthly_motorized_traffic = motorized_traffic_df.groupby('Month').size().rename('Motorized_Traffic')

# Process and filter Non-Motorized Traffic
non_motorized_traffic_df['Month'] = pd.to_datetime(non_motorized_traffic_df['DateTimeFrom'], utc=True).dt.tz_convert(None).dt.to_period('M')
non_motorized_traffic_df = non_motorized_traffic_df[(non_motorized_traffic_df['Month'] >= start_period) & (non_motorized_traffic_df['Month'] <= end_period)]
monthly_non_motorized_traffic = non_motorized_traffic_df.groupby('Month').size().rename('Non_Motorized_Traffic')

# Process and filter Public Holidays
public_holidays_df['Month'] = pd.to_datetime(public_holidays_df['Datum'], format='%d.%m.%Y').dt.to_period('M')
public_holidays_df = public_holidays_df[(public_holidays_df['Month'] >= start_period) & (public_holidays_df['Month'] <= end_period)]
public_holidays_monthly = public_holidays_df.groupby('Month').size().rename('Public_Holidays')

# Combine all monthly data into a single DataFrame
combined_df = pd.concat([
    monthly_measurements,
    monthly_accidents,
    weather_monthly,
    monthly_motorized_traffic,
    monthly_non_motorized_traffic,
    public_holidays_monthly
], axis=1)

# Fill NaN values with 0
combined_df.fillna(0, inplace=True)

print(combined_df)

combined_df[['Year', 'Month']] = combined_df['Month'].str.split('-', expand=True)

# Sorting by Year and then Month

combined_df = combined_df.sort_values(by=['Year', 'Month'])

combined_df.head()  # Displaying the first few rows of the corrected DataFrame


print(combined_df)

combined_df.to_csv('final_2021to23.csv')


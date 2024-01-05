from sqlalchemy import create_engine
import pandas as pd

# Set options
pd.set_option('display.max_columns', None)

# Paths and database credentials
combined_df_path = 'xxx'
host = "database-1.cjc6ya4a0eqo.us-east-1.rds.amazonaws.com"
port = "5432"
dbname = "postgres"
user = "Alexis"
password = "HalloFabian"

# Load the combined DataFrame
combined_df = pd.read_csv(combined_df_path, sep=',')

# Splitting into separate tables
weather = combined_df[['Year', 'Month', 'Temperature', 'Rain']]
traffic = combined_df[['Year', 'Month', 'Motorized_Traffic', 'Non_Motorized_Traffic', 'Speed_Measurements']]
accidents = combined_df[['Year', 'Month', 'Traffic_Accidents']]
public_holidays = combined_df[['Year', 'Month', 'Public_Holidays']]

# Connection string for SQLAlchemy
engine_string = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

# Connect using SQLAlchemy to use with pandas
try:
    # Create an engine instance
    engine = create_engine(engine_string)

    # Connect to PostgreSQL server
    conn = engine.connect()

    # Insert data from each DataFrame to its respective table
    weather.to_sql('weather_table', conn, if_exists='append', index=False)
    traffic.to_sql('traffic_table', conn, if_exists='append', index=False)
    accidents.to_sql('accidents_table', conn, if_exists='append', index=False)
    public_holidays.to_sql('public_holidays_table', conn, if_exists='append', index=False)

    # Close the connection
    conn.close()

except Exception as e:
    print("Database connection failed due to {}".format(e))

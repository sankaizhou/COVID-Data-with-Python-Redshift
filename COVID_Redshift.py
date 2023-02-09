'''
Created on 2 Feb 2023

@author: ariel
'''
import boto3
import pandas as pd
from io import StringIO
import time
import redshift_connector
from future.backports.test.pystone import TRUE


AWS_ACCESS_KEY = "************************"
AWS_SECRET_KEY = "************************"
AWS_REGION = "us-east-2"
SCHEMA_NAME = "covid_dateset"
S3_STAGING_DIR = "s3://covid-result-bucket/Unsaved/"
S3_BUCKET_NAME = "covid-result-bucket"
S3_OUTPUT_DIRECTORY = "Unsaved"

athena_client = boto3.client(
    "athena",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

Dict = {}
def download_and_load_query_results(
        client: boto3.client, query_response: Dict
) -> pd.DataFrame:
    while True:
        try:
            '''This function only loads the first 1000 rows'''
            client.get_query_results(
                QueryExecutionId=query_response["QueryExecutionId"]
            )
            break
        except Exception:
            print('error occured')
            break
    temp_file_location: str="athena_query_results.csv"
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )
    s3_client.download_file(
        S3_BUCKET_NAME,
        f"{S3_OUTPUT_DIRECTORY}/{query_response['QueryExecutionId']}.csv",
        temp_file_location
    )
    return pd.read_csv(temp_file_location)


'''**************************************Create each individual Table***********************************************'''
#table 1)create enigma_jhub table
response1 = athena_client.start_query_execution(
    QueryString="SELECT * FROM enigma_jhud",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)
enigma_jhud = download_and_load_query_results(athena_client, response1)


#table 2)create enigma_nytimes_us_country table
response2 = athena_client.start_query_execution(
    QueryString="SELECT * FROM enigma_nytimes_us_country",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)
enigma_nytimes_us_country = download_and_load_query_results(athena_client, response2)

#table 3)create enigma_nytimes_us_states table
response3 = athena_client.start_query_execution(
    QueryString="SELECT * FROM enigma_nytimes_us_states",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)
enigma_nytimes_us_states = download_and_load_query_results(athena_client, response3)

#table 4)create rearc_test_data_states_daily table
response4 = athena_client.start_query_execution(
    QueryString="SELECT * FROM rearc_test_data_states_daily",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)
rearc_test_data_states_daily = download_and_load_query_results(athena_client, response4)

#table 5)create rearc_test_data_us table
response5 = athena_client.start_query_execution(
    QueryString="SELECT * FROM rearc_test_data_us",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)
rearc_test_data_us = download_and_load_query_results(athena_client, response5)

#table 6)create rearc_test_data_us_daily table
response6 = athena_client.start_query_execution(
    QueryString="SELECT * FROM rearc_test_data_us_daily",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)
rearc_test_data_us_daily = download_and_load_query_results(athena_client, response6)

#table 7)create rearc_usa_hospital_beds table
response7 = athena_client.start_query_execution(
    QueryString="SELECT * FROM rearc_usa_hospital_beds",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)
rearc_usa_hospital_beds = download_and_load_query_results(athena_client, response7)

#table 8)create static_datasets_countrycodeqa_country_population table
response8 = athena_client.start_query_execution(
    QueryString="SELECT * FROM static_datasets_countrycodeqa_country_population",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)
static_datasets_countrycodeqa_country_population = download_and_load_query_results(athena_client, response8)


#table 9)create static_datasets_countrycodeqa_states_abv table
response9 = athena_client.start_query_execution(
    QueryString="SELECT * FROM static_datasets_countrycodeqa_states_abv",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)
static_datasets_countrycodeqa_states_abv = download_and_load_query_results(athena_client, response9)
new_header = static_datasets_countrycodeqa_states_abv.iloc[0] #replace the header because GLUE cannot identify it properly
static_datasets_countrycodeqa_states_abv = static_datasets_countrycodeqa_states_abv[1:]
static_datasets_countrycodeqa_states_abv.column = new_header

#table 10)create static_datasets_countrycodeqs table
response10 = athena_client.start_query_execution(
    QueryString="SELECT * FROM static_datasets_countrycodeqs",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)
static_datasets_countrycodeqs = download_and_load_query_results(athena_client, response10)

'''**************************************ETL: Create FactCovid Table***********************************************'''
factCovid_1 = enigma_jhud[['flips', 'province_state', 'country_region', 'confirmed', 'deaths', 'recovered', 'active']]
factCovid_2 = rearc_test_data_states_daily[['flips', 'date', 'positive', 'negative', 'hospitalized', 'hospitalizeddischarged']]
factCovid = pd.merge(factCovid_1, factCovid_2, on='flips', how='inner')



'''**************************************ETL: Create dimRegion Table***********************************************'''
dimRegion_1 = enigma_jhud[['flips', 'province_state', 'country_region', 'latitude', 'longitude']]
dimRegion_2 = enigma_nytimes_us_country[['flips', 'country', 'state']]
dimRegion = pd.merge(dimRegion_1, dimRegion_2, on='flips', how='inner')


'''**************************************ETL: Create dimHospital Table***********************************************'''
dimHospital = rearc_usa_hospital_beds[['flips', 'state_name', 'latitude', 'longtitude', 'hq_address', 'hospital_name', 'hospital_type', 'hq_city', 'hq_state']]


'''**************************************ETL: Create dimDate Table***********************************************'''
dimDate = rearc_test_data_states_daily[['flips', 'date']]
dimDate['date'] = pd.to_datetime(dimDate['date'], format='%Y%m%d')
dimDate['year'] = dimDate['date'].dt.year #add column year
dimDate['month'] = dimDate['date'].dt.month #add column month
dimDate['date_of_week'] = dimDate['date'].dt.dayofweek #add column day_of_week
#final schema is finished

'''**************************************store the tables in S3 bucket 'output'***********************************************'''
bucket = 'aws-covid-project-sankai' #already created on S3
csv_buffer = StringIO()

#store factCovid table in S3
factCovid.to_csv(csv_buffer)
s3_resource1 = boto3.resource('s3')
s3_resource1.Object(bucket, 'output/factCovid.csv').put(Body=csv_buffer.getvalue())

#store dimRegion table in S3
dimRegion.to_csv(csv_buffer)
s3_resource2 = boto3.resource('s3')
s3_resource2.Object(bucket, 'output/dimRegion.csv').put(Body=csv_buffer.getvalue())

#store dimHospital table in S3
dimHospital.to_csv(csv_buffer)
s3_resource3 = boto3.resource('s3')
s3_resource3.Object(bucket, 'output/dimHospital.csv').put(Body=csv_buffer.getvalue())

#store dimDate table in S3
dimDate.to_csv(csv_buffer)
s3_resource4 = boto3.resource('s3')
s3_resource4.Object(bucket, 'output/dimDate.csv').put(Body=csv_buffer.getvalue())


'''**************************************copy tables from S3 to Redshift***********************************************'''
#get the schema of factCovid table
factCovidsql = pd.io.sql.get_schema(factCovid.reset_index(), 'factCovid')
print(''.join(factCovidsql))

#get the schema of dimRegion table
dimRegionsql = pd.io.sql.get_schema(dimRegion.reset_index(), 'factCovid')
print(''.join(dimRegionsql))

#get the schema of dimHospital table
dimHospitalsql = pd.io.sql.get_schema(dimHospital.reset_index(), 'factCovid')
print(''.join(dimHospitalsql))

#get the schema of dimDate table
dimDatesql = pd.io.sql.get_schema(dimDate.reset_index(), 'dimDate')
print(''.join(dimDatesql))

#connect to Redshift and create table structures in Redshift
conn = redshift_connector.connect(
    host='redshift-cluster-1.cavxvccrwzto.us-east-2.redshift.amazonaws.com:5439/dev',
    database='dev',
    user='sankaizhou',
    password='Mn383301534!'
)

conn.autocommit = True 
cursor = redshift_connector.Cursor = conn.cursor()
cursor.execute('''
CREATE TABLE 'dimDate'(
'index' INTEGER,
'flips' REAL,
'date' TIMESTAMP,
'year' INTEGER,
'month' INTEGER,
'day_of_week' INTEGER
)
''')

cursor.execute('''
CREATE TABLE 'dimHospital'(
'index' INTEGER,
'flips' REAL,
'state_name' TEXT,
'latitude' REAL,
'lontitude' REAL,
'hq_address' TEXT,
'hospital_name' TEXT,
'hospital_type' TEXT,
'hq_city' TEXT,
'hq_state' TEXT
)
''')

cursor.execute('''
CREATE TABLE 'factCovid'(
'index' INTEGER,
'flips' REAL,
'province_state' TEXT,
'country_region' TEXT,
'confirmed' REAL,
'deaths' REAL,
'recovered' REAL,
'active' REAL,
'date' INTEGER,
'positive' REAL,
'negative' REAL,
'hospitalizedcurrently' REAL,
'hospitalized' REAL,
'hospitalizeddischarged' REAL
)
''')

cursor.execute('''
CREATE TABLE 'dimRegion'(
'index' INTEGER,
'flips' REAL,
'province_state' TEXT,
'country_region' TEXT,
'latitude' REAL,
'longtitude' REAL,
'country' TEXT,
'state' TEXT
)
''')

#copy tables from S3 to Redshift
cursor.execute('''
copy dimDate from 'path of the dimDate'
credentials 'IAM role credentials'
delimiter ','
region 'us-east-2'
IGNOREHEADER 1
''')

cursor.execute('''
copy dimDate from 'path of the dimHospital'
credentials 'IAM role credentials'
delimiter ','
region 'us-east-2'
IGNOREHEADER 1
''')

cursor.execute('''
copy dimDate from 'path of the factCovid'
credentials 'IAM role credentials'
delimiter ','
region 'us-east-2'
IGNOREHEADER 1
''')

cursor.execute('''
copy dimDate from 'path of the dimRegion'
credentials 'IAM role credentials'
delimiter ','
region 'us-east-2'
IGNOREHEADER 1
''')



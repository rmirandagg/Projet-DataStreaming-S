from kafka import KafkaConsumer
from time import sleep
from json import dumps,loads
from s3fs import S3FileSystem
import boto3
import json
from botocore.exceptions import NoCredentialsError
import toml #library to load my configuration files
from datetime import datetime


def convert_date_format(payload):
    # Reemplaza 'your_date_field' con el nombre real de tu campo de fecha
    if 'time' in payload:
        #payload['time'] = datetime.strptime(str(payload['time']), '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d %H:%M:%S')
        print("entro")
        timestamp_milliseconds = payload['time'] 
        timestamp_seconds = timestamp_milliseconds / 1000.0

        python_datetime = datetime.utcfromtimestamp(timestamp_seconds)

        print(python_datetime)
       
    return payload


# Configure AWS credentials
app_config = toml.load('config.toml') #loading aws configuration files
access_key = app_config['s3']['keyid'] #getting access key id from config.toml file
secret_access_key = app_config['s3']['keysecret'] #getting access key secrets from config.toml file

payload= []
# Configure  region of S3
region = 'us-east-2'

# Create an instance of the S3 client
s3_client = boto3.client('s3',aws_access_key_id= access_key , aws_secret_access_key=secret_access_key ,region_name= region)
s3= S3FileSystem()


# Configura el consumidor de Kafka
consumer = KafkaConsumer(
    'dbserver1.public.stock',  # Cambia esto con el nombre de tu tema Debezium
    bootstrap_servers='localhost:29092',  # Cambia esto con la dirección de tus servidores Kafka
    group_id='my_group',  # Cambia esto con el ID de tu grupo de consumidores
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Fields
fields_sel = ['record_id','time', 'open', 'low', 'close','volume','high', 'symbol','event_time']
# Iterate through the elements in consumer for count, i in enumerate(consumer):
for count,msg in enumerate(consumer):
    # try:

        # Generate file path in S3
        key = "bucketkafkatest/amzn_stock_kafka_test{}.json".format(count)

        # Convert Python object to JSON format
        # Selecciona solo los campos de interés
        payload = msg.value 
        #print(payload['payload']['time'])
        
        fields_sel = {campo: payload['payload']['after'][campo] for campo in fields_sel if campo in payload['payload']['after']}
        fields_sel = convert_date_format(fields_sel)
        print(fields_sel)
        json_data =  json.dumps(fields_sel)

        #Upload JSON file to S3
        s3_client.put_object(Bucket="bucketkafkatest" ,Key = key, Body=json_data)

        print(f"File {key} successfully stored in S3")
"""
    except NoCredentialsError:

        print("No valid credentials will be found to access S3.")
    except Exception as e:
        print(f"Error storing the file {key} in S3: {e}")

    except KeyboardInterrupt:
        pass

    finally:
        # Cierra el consumidor
        consumer.close()
"""


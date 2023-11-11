from kafka import KafkaConsumer
import json

# Configura el consumidor de Kafka
consumer = KafkaConsumer(
    'dbserver1.public.stock',  # Cambia esto con el nombre de tu tema Debezium
    bootstrap_servers='localhost:29092',  # Cambia esto con la dirección de tus servidores Kafka
    group_id='my_group',  # Cambia esto con el ID de tu grupo de consumidores
    auto_offset_reset='earliest'
)

# Campos de interés
campos_interes = ['timestamp', 'high', 'symbol']

# Consume mensajes
try:
    for msg in consumer:
        payload = json.loads(msg.value.decode('utf-8'))

        # Selecciona solo los campos de interés
        datos_interesantes = {campo: payload['payload']['after'][campo] for campo in campos_interes if campo in payload['payload']['after']}

        print('Nuevo mensaje: {}'.format(datos_interesantes))

except KeyboardInterrupt:
    pass

finally:
    # Cierra el consumidor
    consumer.close()
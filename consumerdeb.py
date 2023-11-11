from kafka import KafkaConsumer
import json

# Configuración del consumidor
bootstrap_servers = 'your_kafka_bootstrap_servers'
bootstrap_servers='localhost:29092'
topic_name = 'dbserver1.public.stock'

# Campos específicos que deseas mostrar
desired_fields = ['timestamp', 'high', 'symbol']

# Crea un consumidor de Kafka
consumer = KafkaConsumer(
    topic_name,
    group_id='my-group',  # Cambia 'my-group' a un ID de grupo único
    bootstrap_servers=[bootstrap_servers],
    auto_offset_reset='earliest',  # Puedes cambiar esto a 'latest' según tus necesidades
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)



# Bucle principal para consumir mensajes
for message in consumer:
    # Decodifica y procesa el mensaje
    value = message.value
    
    # Filtra y muestra solo los campos deseados
    filtered_data = {field: value.get(field, None) for field in desired_fields}
    print(f"Received message: {filtered_data}")

# Cierra el consumidor cuando hayas terminado
consumer.close()
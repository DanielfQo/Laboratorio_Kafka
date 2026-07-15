from kafka import KafkaProducer
import time

BOOTSTRAP = "172.31.19.42:9092"
TOPIC     = "prueba"

# Conexión al broker Kafka
producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP]
)

print("Producer iniciado. Escribe mensajes (Ctrl+C para salir):")

try:
    while True:
        mensaje = input("> ")
        if not mensaje.strip():
            continue

        # Enviar mensaje al topic
        future = producer.send(
            TOPIC,
            value=mensaje.encode("utf-8")
        )

        # Esperar confirmación
        metadata = future.get(timeout=10)

        print(
            f"Mensaje enviado | "
            f"Topic: {metadata.topic} | "
            f"Partition: {metadata.partition} | "
            f"Offset: {metadata.offset}"
        )

except KeyboardInterrupt:
    print("\nProducer detenido.")
finally:
    producer.close()

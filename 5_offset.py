
from kafka import KafkaConsumer

BOOTSTRAP = "172.31.19.42:9092"
TOPIC     = "prueba"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[BOOTSTRAP],
    auto_offset_reset="earliest"
)

print("=" * 55)
print("  CONSUMER con OFFSET - topic 'prueba'")
print("  Muestra la posicion (offset) de cada mensaje.")
print("  Ctrl+C para salir.")
print("=" * 55)

try:
    for mensaje in consumer:
        print(
            f"  Partition: {mensaje.partition}"
            f"  Offset: {mensaje.offset:>4}"
            f"  Mensaje: {mensaje.value.decode()}"
        )

except KeyboardInterrupt:
    print("\n  Consumer detenido.")
finally:
    consumer.close()


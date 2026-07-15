
from kafka import KafkaConsumer

BOOTSTRAP = "172.31.19.42:9092"
TOPIC     = "prueba"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[BOOTSTRAP],
    auto_offset_reset="earliest"   # leer desde el primer mensaje
)

print("=" * 50)
print("  CONSUMER escuchando el topic 'prueba'")
print("  Esperando mensajes... (Ctrl+C para salir)")
print("=" * 50)

try:
    for mensaje in consumer:
        print(f"  [RECIBIDO] -> {mensaje.value.decode()}")

except KeyboardInterrupt:
    print("\n  Consumer detenido.")
finally:
    consumer.close()

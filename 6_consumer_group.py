import socket
from kafka import KafkaConsumer

BOOTSTRAP    = "172.31.19.42:9092"
TOPIC        = "prueba"
GROUP_ID     = "grupo1"
HOSTNAME     = socket.gethostname()   # identifica cual instancia es

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[BOOTSTRAP],
    group_id=GROUP_ID,                 # <- mismo grupo en todos los terminales
    auto_offset_reset="earliest"
)

print("=" * 60)
print(f"  CONSUMER GROUP '{GROUP_ID}'")
print(f"  Instancia : {HOSTNAME}")
print(f"  Topic     : {TOPIC}")
print()
print("  Kafka asignara automaticamente las particiones")
print("  disponibles a esta instancia del grupo.")
print("  Abre otro terminal y ejecuta este mismo script")
print("  para ver como se reparten las particiones.")
print("  Ctrl+C para salir.")
print("=" * 60)

try:
    for mensaje in consumer:
        print("\n[Mensaje Procesado en Grupo]")
        print(f"   Instancia: {HOSTNAME}")
        print(f"   Partition: {mensaje.partition} (Asignada a esta instancia)")
        print(f"   Offset   : {mensaje.offset:<4}")
        print(f"   Mensaje  : {mensaje.value.decode('utf-8')}")

except KeyboardInterrupt:
    print("\nConsumer detenido.")
finally:
    consumer.close()


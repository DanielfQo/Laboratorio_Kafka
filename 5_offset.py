from kafka import KafkaConsumer

BOOTSTRAP = "172.31.19.42:9092"
TOPIC     = "prueba"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[BOOTSTRAP],
    auto_offset_reset="earliest"
)

print("=" * 60)
print("  CONSUMER - DEMOSTRACION DE OFFSET")
print(f"  Escuchando topic : {TOPIC}")
print("  Esperando mensajes... (Ctrl+C para salir)")
print("=" * 60)

try:
    for mensaje in consumer:
        print("\n[Mensaje Recibido]")
        print(f"   Mensaje  : {mensaje.value.decode('utf-8')}")
        print(f"   Partition: {mensaje.partition} (La particion donde se guardo)")
        print(f"   Offset   : {mensaje.offset:<4} (La posicion secuencial en esta particion)")

except KeyboardInterrupt:
    print("\nConsumer detenido.")
finally:
    consumer.close()


from kafka import KafkaConsumer
from datetime import datetime

BOOTSTRAP = "172.31.19.42:9092"
TOPIC     = "prueba"

# Configuración del Consumer
# auto_offset_reset='earliest' indica que si no hay un offset guardado para este grupo,
# comenzara a leer desde el inicio de la partición (primer mensaje disponible).
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[BOOTSTRAP],
    auto_offset_reset="earliest",
    enable_auto_commit=True      # Confirma automaticamente los mensajes leídos
)

print("=" * 60)
print("  CONSUMER iniciado")
print(f"  Escuchando topic : {TOPIC}")
print(f"  Bootstrap Servers: {BOOTSTRAP}")
print("  Esperando mensajes... (Ctrl+C para salir)")
print("=" * 60)

try:
    for mensaje in consumer:
        # Convertir timestamp del mensaje a hora legible
        fecha_msg = datetime.fromtimestamp(mensaje.timestamp / 1000.0).strftime('%H:%M:%S')
        
        print("\n[Nuevo Mensaje Recibido]")
        print(f"   Contenido: {mensaje.value.decode('utf-8')}")
        print(f"   Partition: {mensaje.partition}  (La división del topic de la que provino)")
        print(f"   Offset   : {mensaje.offset:<4} (La posición secuencial del mensaje)")
        print(f"   Hora Msg : {fecha_msg}  (Cuándo fue publicado por el Producer)")

except KeyboardInterrupt:
    print("\nConsumer detenido.")
finally:
    consumer.close()

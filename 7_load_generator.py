import json
import random
import time
from kafka import KafkaProducer

BOOTSTRAP = "172.31.19.42:9092"
TOPIC     = "prueba"

# Serializador para enviar datos en formato JSON
def json_serializer(data):
    return json.dumps(data).encode("utf-8")

print(f"Conectando a Kafka en {BOOTSTRAP}...")
try:
    producer = KafkaProducer(
        bootstrap_servers=[BOOTSTRAP],
        value_serializer=json_serializer,
        # Configuraciones de optimización para alto rendimiento (High Throughput)
        acks=1,                    # Confirmación del líder (equilibrio velocidad/fiabilidad)
        compression_type='gzip',   # Comprimir los datos para optimizar el ancho de banda
        linger_ms=10,              # Esperar hasta 10ms para agrupar mensajes antes de enviar
        batch_size=65536           # Agrupar mensajes en lotes de hasta 64 KB
    )
except Exception as e:
    print(f"Error al conectar con el broker de Kafka: {e}")
    exit(1)

print(f"\nGenerador de carga iniciado para el topic: '{TOPIC}'")
print("Enviando ráfagas de datos de sensores simulados... (Ctrl+C para detener)")

count = 0
start_time = time.time()

try:
    while True:
        # Generar datos simulados de un sensor IoT
        payload = {
            "device_id": f"sensor-{random.randint(1, 100)}",
            "timestamp": time.time(),
            "temperature": round(random.uniform(15.0, 35.0), 2),
            "humidity": round(random.uniform(30.0, 90.0), 2),
            "status": random.choice(["OK", "WARNING", "ERROR"]),
            "sequence": count
        }

        # Envío asíncrono (sin bloquear el bucle)
        producer.send(TOPIC, value=payload)
        count += 1

        # Mostrar progreso cada 2000 mensajes
        if count % 2000 == 0:
            elapsed = time.time() - start_time
            rate = count / elapsed
            print(f"Mensajes enviados: {count:<8} | Velocidad promedio: {rate:.2f} msg/seg")

        # Controlar la velocidad del envío:
        # - Para máxima velocidad de estrés, comenta el time.sleep
        # - 0.001 segundos de pausa permite una alta tasa sin colapsar el CPU
        time.sleep(0.001)

except KeyboardInterrupt:
    print("\n[!] Generador de carga detenido por el usuario.")
finally:
    print("Enviando mensajes pendientes en buffer (flush)...")
    if 'producer' in locals():
        producer.flush()
        producer.close()
    print("Producer cerrado correctamente.")

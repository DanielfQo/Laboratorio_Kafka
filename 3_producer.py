
from kafka import KafkaProducer

BOOTSTRAP = "172.31.19.42:9092"
TOPIC     = "prueba"

producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP]
)

print("=" * 50)
print("  PRODUCER conectado al topic 'prueba'")
print("  Escribe un mensaje y presiona Enter.")
print("  Ctrl+C para salir.")
print("=" * 50)

try:
    while True:
        mensaje = input("\nMensaje: ")
        if not mensaje.strip():
            continue

        producer.send(TOPIC, mensaje.encode())
        producer.flush()
        print(f"  [ENVIADO] -> '{mensaje}'")

except KeyboardInterrupt:
    print("\n  Producer detenido.")
finally:
    producer.close()



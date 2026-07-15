import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

BOOTSTRAP = "172.31.19.42:9092"
TOPIC     = "user-events"

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

print(f"Conectando a Kafka en {BOOTSTRAP}...")
try:
    producer = KafkaProducer(
        bootstrap_servers=[BOOTSTRAP],
        value_serializer=json_serializer,
        acks=1,
        linger_ms=10
    )
except Exception as e:
    print(f"Error al conectar con el broker de Kafka: {e}")
    exit(1)

print(f"\nGenerador de eventos e-commerce iniciado para el topic: '{TOPIC}'")
print("Enviando eventos... (Ctrl+C para detener)")

users = [f"USR{i:03d}" for i in range(1, 200)]
events = ["VIEW_PRODUCT", "ADD_CART", "PURCHASE", "SEARCH"]
products = {
    "Laptop Lenovo": ("Electronics", 3200),
    "Laptop Dell": ("Electronics", 3500),
    "iPhone 15": ("Electronics", 4500),
    "Teclado Mecánico": ("Electronics", 250),
    "Monitor LG 27": ("Electronics", 1200),
    "Cafetera Nespresso": ("Appliances", 800),
    "Licuadora Oster": ("Appliances", 350),
    "Zapatillas Nike Air": ("Footwear", 600),
    "Mochila Antirrobo": ("Accessories", 150)
}
cities = ["Arequipa", "Lima", "Cusco", "Trujillo", "Piura"]

count = 0
try:
    while True:
        user = random.choice(users)
        event = random.choices(events, weights=[50, 25, 10, 15], k=1)[0]
        product, (category, price) = random.choice(list(products.items()))
        city = random.choice(cities)
        
        payload = {
            "user": user,
            "event": event,
            "product": product,
            "category": category,
            "price": price,
            "city": city,
            "timestamp": datetime.now().isoformat()
        }

        producer.send(TOPIC, value=payload)
        count += 1
        
        if count % 10 == 0:
            print(f"[{count}] Evento enviado: {event} | {product} | {user}")
            
        time.sleep(1.0) # Enviar un evento por segundo

except KeyboardInterrupt:
    print("\n[!] Productor detenido.")
finally:
    if 'producer' in locals():
        producer.flush()
        producer.close()
    print("Producer cerrado correctamente.")

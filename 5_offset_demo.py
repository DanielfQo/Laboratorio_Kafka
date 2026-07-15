from kafka import KafkaConsumer, TopicPartition
import time

BOOTSTRAP = "172.31.19.42:9092"
TOPIC     = "prueba"

# Instanciar el consumidor
consumer = KafkaConsumer(
    bootstrap_servers=[BOOTSTRAP],
    auto_offset_reset="earliest"
)

# Asignar manualmente el topic para poder controlar los offsets
# En Kafka, para poder mover los offsets (hacer seek), el consumidor debe tener asignadas las particiones.
print("Conectando al cluster y obteniendo particiones...")
try:
    partition_ids = consumer.partitions_for_topic(TOPIC)
    if not partition_ids:
        print(f"Error: No se encontraron particiones para el topic '{TOPIC}'.")
        consumer.close()
        exit(1)
    partitions = [TopicPartition(TOPIC, p) for p in partition_ids]
    consumer.assign(partitions)
except Exception as e:
    print(f"Error al conectar: {e}")
    consumer.close()
    exit(1)

def mostrar_mensajes():
    """Lee y muestra los mensajes disponibles en el buffer a partir del offset actual."""
    records = consumer.poll(timeout_ms=2000)
    if not records:
        print("  [INFO] No hay mas mensajes en este offset.")
        return
    
    for tp, messages in records.items():
        for msg in messages:
            print(f"  -> [Particion {msg.partition}] Offset: {msg.offset:<3} | Mensaje: {msg.value.decode('utf-8')}")

while True:
    print("\n" + "=" * 60)
    print("  DEMO INTERACTIVA: ¿PARA QUE SERVE EL OFFSET?")
    print("=" * 60)
    print("  El Offset es el puntero de lectura. Puedes moverlo para:")
    print("  1. Reprocesar todo desde el principio (Time Travel / Replay)")
    print("  2. Saltar a un offset especifico en la Particion 0 (Auditoria)")
    print("  3. Ignorar el historial y leer solo mensajes nuevos")
    print("  4. Salir")
    print("-" * 60)
    
    try:
        opcion = input("Elige una opcion (1-4): ").strip()
    except KeyboardInterrupt:
        break
    
    if opcion == "1":
        print("\n[Accion] Moviendo los offsets de todas las particiones al inicio (0)...")
        for tp in partitions:
            consumer.seek_to_beginning(tp)
        print("Leyendo mensajes desde el principio:")
        mostrar_mensajes()
        
    elif opcion == "2":
        try:
            target_offset = int(input("\nIntroduce el Offset al que quieres saltar en la Particion 0: "))
            tp_zero = TopicPartition(TOPIC, 0)
            if tp_zero in partitions:
                consumer.seek(tp_zero, target_offset)
                print(f"[Accion] Puntero de Particion 0 movido a Offset: {target_offset}")
                print("Leyendo mensajes a partir de esa posicion:")
                mostrar_mensajes()
            else:
                print("La particion 0 no esta disponible.")
        except ValueError:
            print("Por favor, introduce un numero valido.")
        except Exception as e:
            print(f"Error: {e}")
            
    elif opcion == "3":
        print("\n[Accion] Moviendo los offsets al final (solo mensajes nuevos)...")
        for tp in partitions:
            consumer.seek_to_end(tp)
        print("Esperando a que envies mensajes nuevos desde el Producer (3_producer.py)...")
        mostrar_mensajes()
        
    elif opcion == "4":
        break
    else:
        print("Opcion no valida.")

print("\nSaliendo de la demo.")
consumer.close()

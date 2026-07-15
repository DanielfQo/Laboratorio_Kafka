# ============================================================
#  CONCEPTO 1: TOPIC
#  Un Topic es el canal donde se publican y leen los mensajes.
#
#  Uso: python 1_topic.py
# ============================================================

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

BOOTSTRAP = "172.31.19.42:9092"
TOPIC     = "prueba"

# Crear el topico
admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)

try:
    admin.create_topics([
        NewTopic(name=TOPIC, num_partitions=3, replication_factor=2)
    ])
    print(f"[OK] Topic '{TOPIC}' creado.")
except TopicAlreadyExistsError:
    print(f"[INFO] Topic '{TOPIC}' ya existe.")

# Listar topico
topics = admin.list_topics()
print("\n--- Topics disponibles ---")
for t in sorted(topics):
    print(f"  {t}")

# Describir topico
desc = admin.describe_topics([TOPIC])
print(f"\n--- Descripcion del topic '{TOPIC}' ---")
for topic_meta in desc:
    print(f"  Topic      : {TOPIC}")
    print(f"  Particiones: {len(topic_meta['partitions'])}")
    for p in topic_meta['partitions']:
        print(f"    Partition {p['partition']}  Leader: {p['leader']}  Replicas: {p['replicas']}")

admin.close()


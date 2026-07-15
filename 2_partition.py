
from kafka.admin import KafkaAdminClient

BOOTSTRAP = "172.31.19.42:9092"
TOPIC     = "prueba"

admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
desc  = admin.describe_topics([TOPIC])
admin.close()

print(f"\n--- Particiones del topic '{TOPIC}' ---\n")
for topic_meta in desc:
    for p in topic_meta['partitions']:
        print(f"  Partition : {p['partition']}")
        print(f"  Leader    : {p['leader']}   <- broker que maneja escrituras")
        print(f"  Replicas  : {p['replicas']}  <- brokers con copia de los datos")
        print(f"  Isr       : {p['isr']}       <- replicas sincronizadas")
        print()

print("""
EXPLICACION:
  El topic esta dividido en 3 particiones, lo que permite:
    - Distribuir la carga entre los brokers.
    - Procesar mensajes en paralelo con multiples consumidores.
  Cada particion tiene un lider (broker activo) y una replica
  en el otro broker para tolerancia a fallos.
""")

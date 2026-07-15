
from kafka.admin import KafkaAdminClient

BOOTSTRAP = "172.31.19.42:9092"
TOPIC     = "prueba"

admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
desc  = admin.describe_topics([TOPIC])
admin.close()

print(f"\n--- Particiones del topic '{TOPIC}' ---\n")
for topic_meta in desc:
    for p in topic_meta['partitions']:
        print(f"  Partition : {p['partition_index']}")
        print(f"  Leader    : {p['leader_id']}   <- broker que maneja escrituras")
        print(f"  Replicas  : {p['replica_nodes']}  <- brokers con copia de los datos")
        print(f"  Isr       : {p['isr_nodes']}       <- replicas sincronizadas")
        print()


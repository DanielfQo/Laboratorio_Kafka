#!/bin/bash
set -e

# Actualizar sistema
dnf update -y

# Instalar Java 17 y utilidades
dnf install -y java-17-amazon-corretto wget tar

# Descargar Kafka
cd /opt

rm -rf /opt/kafka
rm -f kafka_2.13-3.9.1.tgz

wget https://archive.apache.org/dist/kafka/3.9.1/kafka_2.13-3.9.1.tgz

tar -xzf kafka_2.13-3.9.1.tgz

mv kafka_2.13-3.9.1 kafka

CONFIG=/opt/kafka/config/server.properties

CLUSTER_ID="M4xv0vFWSJmPqJj3S9R2WQ"

cat > $CONFIG <<EOF
process.roles=broker,controller
node.id=1

controller.quorum.voters=1@172.31.19.42:9093,2@172.31.27.62:9093

listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
advertised.listeners=PLAINTEXT://172.31.19.42:9092

inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER

listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT

log.dirs=/tmp/kraft-combined-logs
metadata.log.dir=/tmp/kraft-combined-logs

num.partitions=3

offsets.topic.replication.factor=2
transaction.state.log.replication.factor=2
transaction.state.log.min.isr=1

default.replication.factor=2
min.insync.replicas=1
EOF

# Formatear almacenamiento
/opt/kafka/bin/kafka-storage.sh format \
-t $CLUSTER_ID \
-c $CONFIG

# Servicio systemd
cat >/etc/systemd/system/kafka.service <<EOF
[Unit]
Description=Apache Kafka
After=network.target

[Service]
Type=simple
User=root
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=always

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable kafka
systemctl start kafka
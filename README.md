# Guía de Uso: Conceptos Clave de Apache Kafka 🚀

Esta guía explica cómo ejecutar y comprender paso a paso los conceptos clave de Apache Kafka utilizando los scripts de Python desarrollados, indicando claramente en qué servidor/instancia debes ejecutar cada comando.

---

## 🖥️ Requisitos Previos

Antes de comenzar, asegúrate de que la biblioteca cliente de Kafka para Python esté instalada en ambas instancias (**EC1** y **EC2**):

```bash
# Ejecutar en EC1 (172.31.19.42) y EC2 (172.31.27.62)
pip install kafka-python
```

---

## 1. Topic (Canal de Mensajes)

Un **Topic** es la categoría o canal lógico donde se envían y almacenan los registros/mensajes. Los productores publican en ellos y los consumidores se suscriben.

### Cómo ejecutar y verificar:
1. **Ejecutar en EC1 (172.31.19.42) o en tu PC local:**
   ```bash
   python 1_topic.py
   ```
2. **Ejecutar en cualquier instancia (EC1 o EC2) usando la herramienta nativa de Kafka:**
   ```bash
   /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server 172.31.19.42:9092
   ```

---

## 2. Partition (Escalabilidad y Paralelismo)

Un Topic se divide físicamente en una o más **Particiones**. Las particiones permiten distribuir los datos del topic entre múltiples brokers para lograr paralelismo en escritura y lectura.

### Cómo verificar:
1. **Ejecutar en EC1 (172.31.19.42) o en tu PC local:**
   ```bash
   python 2_partition.py
   ```
2. **Ejecutar en cualquier instancia (EC1 o EC2) usando la herramienta nativa de Kafka:**
   ```bash
   /opt/kafka/bin/kafka-topics.sh --describe --topic prueba --bootstrap-server 172.31.19.42:9092
   ```
   * **Líder (Leader):** El nodo encargado de recibir lecturas y escrituras para esa partición.
   * **Réplicas (Replicas):** Los nodos espejo que respaldan la partición para asegurar tolerancia a fallos.

---

## 3. Producer (Emisor de Datos)

El **Producer** es la aplicación origen que publica/envía datos a uno o más topics de Kafka.

### Cómo ejecutar:
1. **Ejecutar en EC1 (172.31.19.42) o en tu PC local (Terminal 1):**
   ```bash
   python 3_producer.py
   ```
2. Escribe cualquier mensaje (ej. *Hola*, *Kafka*, *AWS*) y pulsa `Enter` para enviarlo.

---

## 4. Consumer (Receptor de Datos)

El **Consumer** es la aplicación destino que se suscribe a los topics de Kafka para leer los mensajes publicados.

### Cómo ejecutar:
1. **Ejecutar en EC2 (172.31.27.62) o en tu PC local (Terminal 2):**
   ```bash
   python 4_consumer.py
   ```
2. Al escribir mensajes desde la Terminal del **Producer** (`3_producer.py`), verás cómo aparecen instantáneamente en tiempo real en la terminal del **Consumer**.

---

## 5. Offset (Identificador de Posición)

El **Offset** es un número secuencial entero que Kafka asigna a cada mensaje a medida que llega a una partición. Identifica de manera única la posición de un mensaje dentro de esa partición específica.

### Cómo ejecutar:
1. **Ejecutar en EC2 (172.31.27.62) o en tu PC local (Terminal 2):**
   ```bash
   python 5_offset.py
   ```
2. Al llegar mensajes nuevos del producer, verás que cada registro indica exactamente su número de posición (`Offset: X`) dentro de su respectiva partición.

---

## 6. Consumer Group (Procesamiento en Equipo)

Un **Consumer Group** agrupa múltiples consumidores para que colaboren leyendo de un mismo Topic. Kafka se encarga de balancear y repartir de manera equitativa las particiones del Topic entre todos los miembros activos del grupo.

### Cómo probarlo paso a paso:
1. Abre **Terminal A** en **EC1 (172.31.19.42)** y **Terminal B** en **EC2 (172.31.27.62)**.
2. Ejecuta en ambas terminales el mismo script:
   ```bash
   python 6_consumer_group.py
   ```
3. Envía mensajes desde el producer (`python 3_producer.py`). Verás que los mensajes se reparten entre las dos terminales automáticamente (cada terminal recibe solo los mensajes de la partición que tiene asignada).
4. Comprueba cómo Kafka repartió las particiones ejecutando este comando en **cualquier instancia EC1 o EC2**:
   ```bash
   /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 172.31.19.42:9092 --describe --group grupo1
   ```

FROM apache/flink:1.18-scala_2.12-java11

# Download Kafka SQL connector so Flink SQL CLI can query Kafka topics
RUN wget -q -P /opt/flink/lib \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar

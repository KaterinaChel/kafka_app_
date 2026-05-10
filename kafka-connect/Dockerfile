FROM confluentinc/cp-kafka-connect:7.7.1

# Скопировать JMX Exporter в докер образ
COPY jmx_prometheus_javaagent-0.15.0.jar /opt/
# Скопировать настройки JMX Exporter в докер образ
COPY kafka-connect.yml /opt/

# Skip preflight checks that fail with SSL/ACLs
ENV KAFKA_CONNECT_HEAP_OPTS="-Xms256M -Xmx2G"

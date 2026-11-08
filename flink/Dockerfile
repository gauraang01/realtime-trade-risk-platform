FROM flink:2.2.1-scala_2.12-java17

# Flink ships without a Kafka connector. Anything in lib/ is on the classpath
# of every Flink process, and the checksum makes the build fail if the file
# ever differs from the one this project was tested with.
ADD --chown=flink:flink \
    --checksum=sha256:5605c691d11a501382c383fecba37a7a552467da5ab7ba904ef5d6f3d62c5616 \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar \
    /opt/flink/lib/

# Created in the image so the named volume mounted here is owned by the flink
# user, which is the user the image runs as.
RUN mkdir -p /opt/flink/checkpoints

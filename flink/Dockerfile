FROM flink:2.2.1-scala_2.12-java17

# Flink ships without a Kafka connector. Anything in lib/ is on the classpath
# of every Flink process, and the checksum makes the build fail if the file
# ever differs from the one this project was tested with.
ADD --chown=flink:flink \
    --checksum=sha256:5605c691d11a501382c383fecba37a7a552467da5ab7ba904ef5d6f3d62c5616 \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar \
    /opt/flink/lib/

# For lookup joins against Postgres. Since connector 4.x the JDBC connector is
# split into a core JAR and one JAR per database, and the driver is separate.
ADD --chown=flink:flink \
    --checksum=sha256:f8357ab799095e6486315db4019edf63ced762bc8c2eaa63614c7b962755564b \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc-core/4.1.0-2.2/flink-connector-jdbc-core-4.1.0-2.2.jar \
    /opt/flink/lib/
ADD --chown=flink:flink \
    --checksum=sha256:ae6da5769d932123606f1b9e03d90d57cde9c1d0144df6ffa64a9701afb86d6b \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc-postgres/4.1.0-2.2/flink-connector-jdbc-postgres-4.1.0-2.2.jar \
    /opt/flink/lib/
ADD --chown=flink:flink \
    --checksum=sha256:73914527305a40cce504b0d3d90b23caf565136912d607ae8ae7c5895512332c \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.14/postgresql-42.7.14.jar \
    /opt/flink/lib/

# Created in the image so the named volume mounted here is owned by the flink
# user, which is the user the image runs as.
RUN mkdir -p /opt/flink/checkpoints

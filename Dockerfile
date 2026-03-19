# A Docker image for the Analytics JVM SDK FIT performers.
#
# Build from project root with:
#   docker build . --build-arg SDK=<sdk> -t performer
#
# Run with:
#   docker run -e LOG_LEVEL=DEBUG -p 8060:8060 performer

# Valid SDK values: java
ARG SDK=java

FROM maven:3.9.9-eclipse-temurin-21 AS build

WORKDIR /app
COPY . couchbase-analytics-jvm-clients/

WORKDIR /app/couchbase-analytics-jvm-clients
ARG MVN_FLAGS="--batch-mode --no-transfer-progress -Dcheckstyle.skip -Dmaven.test.skip -Dmaven.javadoc.skip"
ARG SDK
RUN mvn $MVN_FLAGS package -Pfit --projects couchbase-analytics-${SDK}-client/fit --also-make

# Multistage build to keep things small
FROM eclipse-temurin:21-jre-ubi10-minimal

ARG SDK
COPY --from=build /app/couchbase-analytics-jvm-clients/couchbase-analytics-${SDK}-client/fit/target/analytics-${SDK}-fit-performer-1.0-SNAPSHOT-jar-with-dependencies.jar performer.jar

ENV LOG_LEVEL=INFO
EXPOSE 8060

ENTRYPOINT ["java", "-jar", "performer.jar"]

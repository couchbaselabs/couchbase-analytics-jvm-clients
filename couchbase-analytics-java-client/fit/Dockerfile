# A Docker image for the Couchbase Analytics SDK test performer for Java.
# Go to a directory containing couchbase-analytics-jvm-clients and transactions-fit-performer.
# Build with this command:
# docker build -f couchbase-analytics-jvm-clients/couchbase-analytics-java-client/fit/Dockerfile .

# Use 'maven' because it has git pre-installed.
# (Could use `eclipse-temurin:21` and run "apt update && apt install -y git" but that's much slower.)
FROM maven:3.9.9-eclipse-temurin-21 AS build

WORKDIR /app
COPY transactions-fit-performer transactions-fit-performer/
COPY couchbase-analytics-jvm-clients couchbase-analytics-jvm-clients/

# The MAVEN_CONFIG environment variable set by `maven` base image interferes with the wrapper, so clear it!
ARG MAVEN_CONFIG=""

ARG MAVEN_ARGS="--batch-mode -Dcheckstyle.skip -Dmaven.test.skip -Dmaven.javadoc.skip"

WORKDIR /app/transactions-fit-performer
RUN ./mvnw --projects gRPC/jvm install

WORKDIR /app/couchbase-analytics-jvm-clients
RUN ./mvnw install
RUN ./mvnw package --file couchbase-analytics-java-client/fit

# Multistage build to keep things small
FROM eclipse-temurin:21-jre

WORKDIR /app
COPY --from=build /app/couchbase-analytics-jvm-clients/couchbase-analytics-java-client/fit/target/analytics-java-fit-performer-1.0-SNAPSHOT-jar-with-dependencies.jar .

ENTRYPOINT ["java", "-jar", "analytics-java-fit-performer-1.0-SNAPSHOT-jar-with-dependencies.jar"]

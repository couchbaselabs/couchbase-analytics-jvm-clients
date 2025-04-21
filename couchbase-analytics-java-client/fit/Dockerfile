# A Docker image for the Couchbase Analytics SDK test performer for Java.
# Build from root with:
# docker build -f couchbase-analytics-jvm-clients/couchbase-analytics-java-client/fit/Dockerfile .

# Use 'maven' because it has git pre-installed.
# (Could use `eclipse-temurin:21` and run "apt update && apt install -y git" but that's much slower.)
FROM maven:3.9.9-eclipse-temurin-21 AS build

WORKDIR /app

COPY transactions-fit-performer transactions-fit-performer/
COPY couchbase-analytics-jvm-clients couchbase-analytics-jvm-clients/

WORKDIR /app/transactions-fit-performer
# The MAVEN_CONFIG environment variable set by `maven` base image interferes with the wrapper, so unset it on first use!
# Need "also-make" so the parent pom gets installed. TODO: flatten the gRPC/jvm pom!
RUN MAVEN_CONFIG= ./mvnw --batch-mode --projects gRPC/jvm --also-make install -Dcheckstyle.skip -Dmaven.test.skip -Dmaven.javadoc.skip

WORKDIR /app/couchbase-analytics-jvm-clients
RUN ./mvnw --batch-mode install -Dmaven.test.skip -Dmaven.javadoc.skip
RUN ./mvnw --batch-mode package -Dmaven.test.skip -Dmaven.javadoc.skip --file couchbase-analytics-java-client/fit

# Multistage build to keep things small
FROM eclipse-temurin:21-jre

WORKDIR /app
COPY --from=build /app/couchbase-analytics-jvm-clients/couchbase-analytics-java-client/fit/target/analytics-java-fit-performer-1.0-SNAPSHOT-jar-with-dependencies.jar .

ENTRYPOINT ["java", "-jar", "analytics-java-fit-performer-1.0-SNAPSHOT-jar-with-dependencies.jar"]

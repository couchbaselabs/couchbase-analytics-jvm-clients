# couchbase-analytics-jvm-clients

JVM clients for Couchbase Enterprise Analytics


## Maven coordinates
```xml
<dependency>
    <groupId>com.couchbase.client</groupId>
    <artifactId>couchbase-analytics-java-client</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

## Snapshot repository

```xml
<repositories>
    <repository>
        <name>Central Portal Snapshots</name>
        <id>central-portal-snapshots</id>
        <url>https://central.sonatype.com/repository/maven-snapshots/</url>
        <releases>
            <enabled>false</enabled>
        </releases>
        <snapshots>
            <enabled>true</enabled>
        </snapshots>
    </repository>
</repositories>
```

## Example Usage

Start by creating a `Cluster` instance:

```java
Cluster cluster = Cluster.newInstance(
  connectionString, // like "https://..."
  Credential.of(username, password),
  // The third parameter is optional.
  // This example sets the default query timeout to 2 minutes.
  clusterOptions -> clusterOptions
    .timeout(it -> it.queryTimeout(Duration.ofMinutes(2)))
);
```

A `Cluster` instance is thread-safe.
For best performance, create a single instance and share it.

When you no longer need the `Cluster` instance, call its `close()` method to release resources.

To execute a query whose results fit in memory:

```java
QueryResult result = cluster.executeQuery(
  "SELECT `hello world` as greeting"
);

for (Row row : result.rows()) {
  String greeting = row.as(ObjectNode.class)
      .path("greeting")
      .textValue();
  
  System.out.println(greeting);
}
```

For more examples, including how to handle large result sets, see the source code in the [Maven project template](couchbase-analytics-java-client/examples/maven-project-template).

## Migrating

See the [Migration Guide](MIGRATING.md) if you're migrating from the Couchbase Java SDK.

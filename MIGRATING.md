# Migration Guide

The Couchbase Analytics Java SDK is designed to work specifically with Couchbase Enterprise Analytics.
It is a successor to the analytics API from the general Couchbase Java SDK, which we'll refer to here as the "operational" SDK.

This section offers advice on how to migrate code from the operational SDK ("before") to the analytics SDK ("after").

## Class names

The analytics SDK omits the "Analytics" prefix from several class names in favor of the more general "Query" prefix.

| Before              | After                         |
|---------------------|-------------------------------|
| `AnalyticsResult`   | `QueryResult`                 |
| `AnalyticsOptions`  | `QueryOptions`                |
| `AnalyticsMetaData` | `QueryMetadata` (lowercase d) |
| `AnalyticsWarning`  | `QueryWarning`                |

The operational SDK's reactive API does not have a direct analogue in the base analytics SDK.
An API for integrating with Project Reactor is available as an extension library.
Note the prefix "Reactive" changes to "Reactor".

| Before                | After (with extension library) |
|-----------------------|--------------------------------|
| `ReactiveQueryResult` | `ReactorQueryResult`           |

## Method names

| Before           | After                                    |
|------------------|------------------------------------------|
| `analyticsQuery` | `executeQuery` / `executeStreamingQuery` |


## Query options

With the operational SDK, the caller creates an instance of `AnalayticsOptions` and configures it.
The operational SDK expected you to pass positional parameters as a `JsonArray`, and named parameters as a `JsonObject`.

With the analytics SDK, options are specified via a callback that modifies an instance of `QueryOptions` created by the SDK.
The analytics SDK takes positional parameters as a `List`, and named parameters as a `Map`.

_Before:_

```java
AnalyticsResult result = operationalCluster
    .analyticsQuery(
        "SELECT ? AS greeting",
        AnalyticsOptions.analyticsOptions()
            .readonly(true)
            .parameters(JsonArray.from("hello world"))
    );
```

_After:_

```java
QueryResult result = analyticsCluster
    .executeQuery(
        "SELECT ? AS greeting",
        options -> options
            .readOnly(true) // uppercase "O"
            .parameters(List.of("hello world"))
        );
```

## JsonObject and JsonArray

These classes are present in both the operational and analytics SDKs, but in different packages.
The two versions have similar methods, but are not interchangeable.

The version to use with the analytics SDK is in package `com.couchbase.analytics.client.java.json`.

If you need to pass JSON between the analytics and operational SDKs, first convert the JSON to a byte array using the `toBytes()` method.
Then use the other SDK's version to parse the JSON using the `fromJson(byte[])` method.


## Converting row values

With the operational SDK, query result rows are accessed by calling `AnalyticsResult.rowsAs(<type>)`.
This method returns a new list where each result row is mapped to an instance of the specified type.

The analytics SDK represents result rows differently.
It introduces a new `Row` class that represents a single result row.
The `queryResult.rows()` method returns a `List<Row>`.
To convert a row to an instance of some type, call `row.as(<type>)`.
If null is a valid value, call `row.asNullable(<type>)` instead.

Unlike the operational SDK, the analytics SDK does not have a dedicated method for converting a row to a `JsonObject`.
Instead, use `row.as(JsonObject.class)`.
(Make sure to use the version of `JsonObject` from the analytics SDK instead of the operational SDK, otherwise the conversion will fail.)

_Before:_

```java
import com.couchbase.client.java.json.JsonObject;
```

```java
AnalyticsResult result = operationalCluster
    .analyticsQuery("SELECT 'hello world' AS greeting");

JsonObject obj = result.rowsAsObject().getFirst();
System.out.println(obj.getString("greeting"));
```

_After:_

```java
import com.couchbase.analytics.client.java.json.JsonObject;
```

```java
QueryResult result = analyticsCluster
    .executeQuery("SELECT 'hello world' AS greeting");
    
JsonObject obj = result.rows().getFirst().as(JsonObject.class);
System.out.println(obj.getString("greeting"));
```

## Streaming result rows

In the operational SDK, the only way to stream result rows from the server is to use the Reactive API.
The analytics SDK adds a safe and convenient way to stream results rows without the complexity of reactive programming.

_Before:_

```java
Mono<ReactiveAnalyticsResult> resultMono = operationalCluster.reactive()
    .analyticsQuery("SELECT RAW i FROM ARRAY_RANGE(0, 10) as i");

resultMono.flatMapMany(result -> result.rowsAs(Integer.class))
    .doOnNext(System.out::println)
    .blockLast();
```

_After:_

```java
analyticsCluster.executeStreamingQuery(
    "SELECT RAW i FROM ARRAY_RANGE(0, 10) as i",
    row -> System.out.println(row.as(Integer.class))
);
```

To aid migration of existing reactive codebases, and to support integrations with other reactive components, the analytics SDK has an optional extension library that adds support for Project Reactor.

```xml
<dependency>
    <groupId>com.couchbase.client</groupId>
    <artifactId>couchbase-analytics-java-client-reactor</artifactId>
    <version>x.y.z</version>
</dependency>
```

_After (with Reactor extension library):_

```java
var reactor = ReactorQueryable.from(analyticsClusterOrScope);

Mono<ReactorQueryResult> resultMono = reactor
    .executeQuery("SELECT RAW i FROM ARRAY_RANGE(0, 10) as i");

resultMono.flatMapMany(ReactorQueryResult::rows)
    .map(row -> row.as(Integer.class))
    .doOnNext(System.out::println)
    .blockLast();
```

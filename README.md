
Apache Flink Connector for JMS
------------------------------

## Artifacts

### Maven

```xml
<dependency>
  <groupId>com.github.miwurster</groupId>
  <artifactId>flink-connector-jms</artifactId>
  <version>${version}</version>
</dependency> 
```

## Usage

* [`JmsQueueSource`](src/test/java/org/apache/flink/streaming/connectors/jms/JmsQueueSourceExample.java)
* [`JmsQueueSink`](src/test/java/org/apache/flink/streaming/connectors/jms/JmsQueueSinkExample.java)
* [`JmsTopicSink`](src/test/java/org/apache/flink/streaming/connectors/jms/JmsTopicSinkExample.java)

## Building

We use Maven as our build system: 

```bash
mvn clean install
```

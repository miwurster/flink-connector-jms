
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



// Create ActiveMQ connection factory
final ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("failover:tcp://localhost:61616");

// Prepare input and output queues
final ActiveMQQueue inputQueue = new ActiveMQQueue("INPUT");
final ActiveMQQueue outputQueue = new ActiveMQQueue("OUTPUT");

final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

env.addSource(new ActiveMQQueueSource(connectionFactory, inputQueue))
  .flatMap(new WordSplitter())
  .keyBy(0)
  .timeWindow(Time.of(5, TimeUnit.SECONDS), Time.of(2, TimeUnit.SECONDS))
  .sum(1)
  .addSink(new ActiveMQQueueSink(connectionFactory, outputQueue))
  .execute("ActiveMQ Example");

private static class WordSplitter implements FlatMapFunction<String, Tuple2<String, Integer>>
{
 @Override
 public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception
 {
   for (String word : value.split(" "))
   {
     out.collect(new Tuple2<>(word, 1));
   }
 }
}



## Building

We use Maven as our build system: 

```bash
mvn clean install
```

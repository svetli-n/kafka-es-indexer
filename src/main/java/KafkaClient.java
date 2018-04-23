import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.log4j.Logger;

public class KafkaClient implements Runnable {
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final KafkaConsumer<String, String> consumer;
  private int id;
  private ElasticIndexer elasticIndexer;

  final static Logger logger = Logger.getLogger(KafkaClient.class);

  public KafkaClient(Properties props, int id, ElasticIndexer elasticIndexer) {
    Properties kafkaProps = new Properties();
    kafkaProps.put("bootstrap.servers", props.getProperty("kafka.bootstrap.servers", "127.0.0.1:9092"));
    kafkaProps.put("enable.auto.commit", props.getProperty("kafka.enable.auto.commit", "true"));
    kafkaProps.put("auto.commit.interval.ms", props.getProperty("kafka.auto.commit.interval.ms", "1000"));
    kafkaProps.put("key.deserializer", props.getProperty("kafka.key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"));
    kafkaProps.put("value.deserializer", props.getProperty("kafka.value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"));
    kafkaProps.put("group.id", props.getProperty("kafka.group.id"));
    if (props.getProperty("loadtest.fromBeginning", "false").equals("true")) {
      kafkaProps.put("auto.offset.reset", "earliest");
      kafkaProps.put("group.id", new Date().toString());
    }
    kafkaProps.put("max.poll.records", props.getProperty("loadtest.maxBatchSize", "100"));

    consumer = new KafkaConsumer<>(kafkaProps);
    String kafkaTopics = (String) props.get("kafka.topics");
    consumer.subscribe(Arrays.asList(kafkaTopics.split(",")));
    this.id = id;
    this.elasticIndexer = elasticIndexer;
  }

  @Override
  public void run() {
    List<String> docs = new ArrayList<>();
    try {
      while (!closed.get()) {
        ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
        for (ConsumerRecord<String, String> record : records) {
          logger.debug(String.format("consumer = %d, offset = %d, key = %s, value = %s%n", id, record.offset(), record.key(), record.value()));
          String json = CefParser.toJson(record.value());
          logger.debug(String.format("converted to json %s", json));
          if (!json.equals("{}")) {
            docs.add(json);
          }
          else {
            logger.debug("json doc is empty");
          }
        }
        if (!docs.isEmpty()) {
          logger.info(String.format("sending %d docs to elaticsearch ", docs.size()));
          elasticIndexer.bulkIndex(docs);
          docs.clear();
        }
        else {
          logger.info("json doc list is empty");
        }
      }
    } catch (WakeupException e) {
      // Ignore exception if closing
      if (!closed.get()) throw e;
    } finally {
      consumer.close();
    }
  }

  // Shutdown hook which can be called from a separate thread
  public void shutdown() {
    elasticIndexer.stop();
    closed.set(true);
    consumer.wakeup();
    logger.info(String.format(String.format("Stopping worker " + id)));
  }

}

package com.swapnil.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;
import java.util.stream.IntStream;

public class SimpleKafkaProducer {
  private final KafkaProducer<String, String> kafkaProducer;
  private final String topicName;

  public static void main(String[] args) throws IOException {
    SimpleKafkaProducer producer = new SimpleKafkaProducer(new Configuration());
    IntStream.range(1,100).forEach(i -> producer.send("Message " + i));
    producer.kafkaProducer.flush();
    producer.kafkaProducer.close();
  }

  public SimpleKafkaProducer(Configuration configuration) {
    topicName = configuration.getProperty("topicName");
    this.kafkaProducer = configureKafkaProducer(configuration);
  }

  private KafkaProducer<String, String> configureKafkaProducer(Configuration configuration) {
    final Properties props = new Properties();
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, configuration.getProperty("producer.key-serializer"));
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        configuration.getProperty("producer.value-serializer"));
    props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "Producer");
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getProperty("bootstrap-servers"));
    return new KafkaProducer<>(props);
  }

  public void send(String message) {
    final ProducerRecord<String, String> record = new ProducerRecord<>(topicName, message);
    kafkaProducer.send(record, (metadata, exception) -> System.out.println(metadata.offset()));
  }
}

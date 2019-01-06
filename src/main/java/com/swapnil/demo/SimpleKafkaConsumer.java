package com.swapnil.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.*;

public class SimpleKafkaConsumer {
  private final KafkaConsumer kafkaConsumer;
  private final String topicName;

  public static void main(String[] args) throws IOException {
    SimpleKafkaConsumer consumer = new SimpleKafkaConsumer(new Configuration());
    while(true) {
      List<String> messages = consumer.receive();
      messages.forEach(System.out::println);
    }
  }

  public SimpleKafkaConsumer(Configuration configuration) {
    topicName = configuration.getProperty("topicName");
    kafkaConsumer = configureKafkaConsumer(configuration);
  }

  private KafkaConsumer configureKafkaConsumer(Configuration configuration) {
    final Properties props = new Properties();
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getProperty("bootstrap-servers"));
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        configuration.getProperty("consumer.key-deserializer"));
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        configuration.getProperty("consumer.value-deserializer"));
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, configuration.getProperty("consumer.group-id"));
    return new KafkaConsumer(props);
  }

  public List<String> receive() {
    final List<String> messages = new ArrayList<>();
    kafkaConsumer.subscribe(Collections.singletonList(topicName));
    ConsumerRecords records = kafkaConsumer.poll(10);
    if(records.count() > 0) {
      Iterator iterator = records.iterator();
      while(iterator.hasNext()) {
        ConsumerRecord record = (ConsumerRecord)iterator.next();
        messages.add((String)record.value());
      }
      kafkaConsumer.commitSync();
    }
    kafkaConsumer.unsubscribe();
    return messages;
  }
}

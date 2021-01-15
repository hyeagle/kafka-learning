package consumer.deserializer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import producer.serializer.User;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDeserializerTest {
    public static void main(String[] args) {
        String topicName = "test";
        String groupId = "test-group";

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "10.213.245.196:9092");
        properties.put("group.id", groupId);
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "latest");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "consumer.deserializer.UserDeserializer");
        KafkaConsumer<String, User> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topicName));

        try {
            while (true) {
                ConsumerRecords<String, User> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, User> record : records) {
                    System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}

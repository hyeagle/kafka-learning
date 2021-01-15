package consumer.multithread;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerThread implements Runnable {

    // 每个线程维护私有的 consumer 实例
    private final KafkaConsumer<String, String> consumer;

    public ConsumerThread(String brokerList, String groupId, String topic) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", brokerList);
        properties.put("group.id", groupId);
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "latest");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        this.consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
    }
    @Override
    public void run() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("thread name = %s, partition = %d, offset = %d, key = %s, value = %s%n",
                        Thread.currentThread().getName(), record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }
}

package producer.partitioner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerPartitionerTest {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();

        properties.put("bootstrap.servers", "10.213.245.196:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "producer.partitioner.CustomPartitioner");

        String topic = "test";
        Producer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> nonKeyRecord = new ProducerRecord<>(topic, "no-key record");
        ProducerRecord<String, String> auditRecord = new ProducerRecord<>(topic, "audit", "audit record");
        ProducerRecord<String, String> nonAuditRecord = new ProducerRecord<>(topic, "other", "non-audit record");

        producer.send(nonKeyRecord).get();
        producer.send(nonAuditRecord).get();
        producer.send(auditRecord).get();
        producer.send(nonKeyRecord).get();
        producer.send(nonAuditRecord).get();

    }
}

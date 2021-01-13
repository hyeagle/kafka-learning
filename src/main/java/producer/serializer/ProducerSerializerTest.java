package producer.serializer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerSerializerTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "10.213.245.196:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "producer.serializer.UserSerializer");

        String topic = "test";
        Producer<String, User> producer = new KafkaProducer<>(properties);
        User user = new User("first", "last", 18, "北京");
        ProducerRecord<String, User> producerRecord = new ProducerRecord<>(topic, user);
        producer.send(producerRecord).get();
        producer.close();
    }
}

package producer.callback;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class ProducerCallbackTest {
    public static void main(String[] args) {
        Properties props = new Properties();

        props.put("bootstrap.servers", "10.213.245.196:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("acks", "-1");
        props.put("retries", 3);

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("test", Integer.toString(i), Integer.toString(i)),
                    (recordMetadata, e) -> System.out.printf("callback, offset =  %d, partition = %d%n", recordMetadata.offset(), recordMetadata.partition()));
        }

        producer.close();
    }
}

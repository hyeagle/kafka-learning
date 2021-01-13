package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerTest {
    public static void main(String[] args) {
        Properties props = new Properties();

        props.put("bootstrap.servers", "10.213.245.196:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("acks", "-1");
        props.put("retries", 3);

        // producer 发往同一个 partition 的数据会封装进一个 batch，batch 满了就发送，空闲时也会发送
        // 默认是 16384，16KB
        // 或者，props.put(ProducerConfig。BATCH _ SIZE_CONFIG, 1048576)
        props.put("batch.size", 323840);
        // 如果 batch 不满，多长时间发送，默认是 0
        // 或者，props.put(ProducerConfig.LINGER_MS_CONFIG, 10)
        props.put("linger.ms", 10);

        // buffer.memory 缓存消息的缓冲区大小，单位字节，33554432 是 32MB。
        // java 版本的 producer 会创建缓冲区用于发送消息，由另一个线程从缓冲区读取并发送。
        // 如果写消息速度超过 IO，producer 会停下等待，如果等待时间过长，会抛出异常
        // 或者，props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432)
        props.put("buffer.memory", 33554432);
        props.put("max.block.ms", 3000);

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<>("test", Integer.toString(i), Integer.toString(i)));
        }


        producer.close();
    }
}

package producer.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class TimeStampPrependerInterceptor implements ProducerInterceptor<String, String> {
    // value 值加上当前的时间戳
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        return new ProducerRecord<>(producerRecord.topic(),
                producerRecord.partition(),
                producerRecord.timestamp(),
                producerRecord.key(),
                System.currentTimeMillis() + "," + producerRecord.value());
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}

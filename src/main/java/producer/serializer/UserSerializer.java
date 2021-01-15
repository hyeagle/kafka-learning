package producer.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class UserSerializer implements Serializer {

    private ObjectMapper objectMapper;

    @Override
    public void configure(Map configs, boolean isKey) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        byte[] ret = null;
        try {
            ret = objectMapper.writeValueAsString(data).getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            System.out.println("failed to serialize the object: " + data);
            e.printStackTrace();
        }
        return ret;
    }
}

package consumer.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import producer.serializer.User;

import java.io.IOException;
import java.util.Map;

public class UserDeserializer implements Deserializer<User> {

    private ObjectMapper objectMapper;

    @Override
    public void configure(Map configs, boolean isKey) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public User deserialize(String s, byte[] bytes) {
        return null;
    }

    @Override
    public User deserialize(String topic, Headers headers, byte[] data) {
        User user = null;
        try {
            user = objectMapper.readValue(data, User.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return user;
    }

    @Override
    public void close() {

    }
}

package consumer.multithread;

public class ConsumerMultiTest {
    public static void main(String[] args) {
        String topicName = "test";
        String groupId = "test-group";
        String brokerList = "10.213.245.196:9092";
        int consumerNum = 3;

        ConsumerGroup consumerGroup = new ConsumerGroup(consumerNum, groupId, topicName, brokerList);
        consumerGroup.execute();
    }
}

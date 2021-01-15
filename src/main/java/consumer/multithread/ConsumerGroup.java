package consumer.multithread;

import java.util.ArrayList;
import java.util.List;

public class ConsumerGroup {

    private List<ConsumerThread> consumerThreads;

    public ConsumerGroup(int consumerNum, String groupId, String topic, String brokerList) {
        consumerThreads = new ArrayList<>(consumerNum);
        for (int i = 0; i < consumerNum; i++) {
            ConsumerThread consumerThread = new ConsumerThread(brokerList, groupId, topic);
            consumerThreads.add(consumerThread);
        }
    }

    public void execute() {
        for (ConsumerThread consumerThread : consumerThreads) {
            new Thread(consumerThread).start();
        }
    }
}

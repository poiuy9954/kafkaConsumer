package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MultiThreadConsumer{

    private final static String TOPIC_NAME ="test";
    private final static String BOOTSTRAP_SERVERS ="localhost:9092";
    private final static String GROUP_ID ="test-group";
    private final static int excuteThreads = 3;

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        ExecutorService executorService = Executors.newCachedThreadPool();
        for (int i=0;i<excuteThreads;i++){
            MultiConsumer multiConsumer = new MultiConsumer(TOPIC_NAME,BOOTSTRAP_SERVERS,GROUP_ID,props);
            executorService.execute(multiConsumer);
        }
    }

}


@Slf4j
class MultiConsumer implements Runnable {


    private final String TOPIC_NAME;
    private final String BOOTSTRAP_SERVERS;
    private final String GROUP_ID;
    private final Properties props;
    private KafkaConsumer<String, String> consumer;

    public MultiConsumer(String topicName, String bootstrapServers, String groupId, Properties props) {
        this.TOPIC_NAME = topicName;
        this.BOOTSTRAP_SERVERS = bootstrapServers;
        this.GROUP_ID = groupId;
        this.props = props;
    }

    @Override
    public void run() {
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                log.info("MutiThreadConsumer record: {}", record);
            }
        }
    }
}

package org.example;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class SyncCommitConsumerForRecord {

    private final static String TOPIC_NAME ="test";
    private final static String BOOTSTRAP_SERVERS ="localhost:9092";
    private final static String GROUP_ID ="test-group";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); //정해진 시간마다 commit하는 기능 비활성

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            for (ConsumerRecord<String, String> record : records) {
                log.info("info msg :: {}",record);
                offsets.put(
                        new TopicPartition(record.topic(),record.partition()),
                        new OffsetAndMetadata(record.offset()+1,null)); //offset +1해야함 poll()호출 시 마지막 커밋 오프셋부터 리턴함
                consumer.commitSync(offsets); //각 record 처리 수행 완료 후 커밋
            }
        }
    }
}



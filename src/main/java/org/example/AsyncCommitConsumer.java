package org.example;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class AsyncCommitConsumer {

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
            for (ConsumerRecord<String, String> record : records) {
                log.info("info msg :: {}",record);
            }
//            consumer.commitAsync(); //비동기로 모든 record 처리 후 커밋 속도 빠름
            consumer.commitAsync(new OffsetCommitCallback() {//비동기 callback으로 처리
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offset, Exception e) {
                    //record 처리 성공 시 onComplete 호출 됨
                    if (e != null) System.err.println("commit error");
                    else System.out.println("commit success");
                    if (e != null) log.error("error commit offset {}",offset, e);
                }
            });
        }
    }
}

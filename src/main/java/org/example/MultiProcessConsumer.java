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

@Slf4j
public class MultiProcessConsumer {

    private final static String TOPIC_NAME ="test";
    private final static String BOOTSTRAP_SERVERS ="localhost:9092";
    private final static String GROUP_ID ="test-group";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        ExecutorService executorService = Executors.newCachedThreadPool();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                ConsumWorkerProcess workerProcess = new ConsumWorkerProcess(record.value());
                executorService.execute(workerProcess);
            }
        }
    }
}

@Slf4j
class ConsumWorkerProcess implements Runnable{
    private String readRecode;

    public ConsumWorkerProcess(String readRecode) {this.readRecode = readRecode;}

    @Override
    public void run() {
        log.info("ConsumProcess -- Run recode :: " + readRecode);
    }
}
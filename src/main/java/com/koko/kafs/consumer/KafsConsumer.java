package com.koko.kafs.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.Topic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;

@Component
public class KafsConsumer implements ApplicationRunner {
    @Value("${topic}")
    String topic;

    @Value("${consumerPollDur}")
    int consumerPollDur;

    @Autowired
    KafkaConsumer<String, String> consumer;

    public void process(ConsumerRecord<String, String> record){
        System.out.printf("Received Message topic =%s, partition =%s, offset = %d, timestamp = %s, key = %s, value = %s\n", record.topic(), record.partition(), record.offset(), record.timestamp(), record.key(), record.value());
    }

    private void printTopicState(){
        System.out.println("Info.");
        consumer.subscription().stream().forEach(System.out::println);
        Set<TopicPartition> theAssignment = Set.of(new TopicPartition(topic, 0), new TopicPartition(topic, 1));

        Map<TopicPartition, OffsetAndMetadata> info = consumer.committed(theAssignment);
        for(Map.Entry<TopicPartition, OffsetAndMetadata> p: info.entrySet()){
            System.out.printf("\t: topic = %s, partition = %s, offset = %d, metadata = %s", p.getKey().topic().toString(), p.getKey().partition(), p.getValue().offset(), p.getValue().metadata());
        }
        System.out.println("\r\nInfo.");
    }

    private void setToTheBeginning(){
        Map<TopicPartition,Long> offsets = consumer.beginningOffsets(Set.of(new TopicPartition(topic, 0), new TopicPartition(topic, 1)));
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        for(Map.Entry<TopicPartition, Long> p: offsets.entrySet())
            currentOffsets.put(p.getKey(), new OffsetAndMetadata(p.getValue(), "start"));
        consumer.commitSync(currentOffsets);
    }

    private Map<TopicPartition, OffsetAndMetadata> initConsumer(){
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

        consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                consumer.commitSync(currentOffsets);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                // do nothing more
            }
        });

        return currentOffsets;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = initConsumer();

        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run(){
                consumer.wakeup();
                try{
                    consumer.close();
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
        });

        try {
            printTopicState();
            //setToTheBeginning();
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(consumerPollDur));
                for (ConsumerRecord<String, String> record : records) {
                    process(record);
                    // Точка уязвимости: если в этом месте происходит сбой, получаем повторную обработку текущей записи record после перезапуска.
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()+1,"cur"));
                }
                if(records.count() > 0) consumer.commitSync(currentOffsets);
            }
        }catch(WakeupException we){
            //we.printStackTrace();
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            try{
                consumer.commitSync(currentOffsets);
            }catch(Exception fe){
                fe.printStackTrace();
            }
            consumer.close();
        }
    }
}

package com.koko.kafs.producer;

import com.koko.kafs.model.InfoItem;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.Future;

import static java.lang.Thread.sleep;

public class KafsProducer implements ApplicationRunner {
    static List<String> names = List.of("bob", "alice", "matthew", "steve", "alex", "ashley", "lora", "authumn", "mary", "olga", "tomas", "lacy", "colin", "gedeon", "toni", "barbara", "john");

    @Value("${topic}")
    String topic;

    @Value("${syncSend}")
    boolean syncSend;

    @Autowired
    private KafkaProducer<String, String> kafkaProducer;

    static class KafsProducerCallback<String> implements Callback{
        @Override
        public void onCompletion(RecordMetadata m, Exception e) {
            if (e != null) {
                e.printStackTrace();
            } else {
                System.out.printf("Produced record to topic %s partition [%d] @ offset %d%n", m.topic(), m.partition(), m.offset());
            }
        }
    }

    @Override
    public void run(ApplicationArguments args) throws Exception{
        int i = 0;
        do {
            InfoItem infoItem = createInfoItem(i++);
            Future<RecordMetadata> recordMetadataFuture = kafkaProducer.send(new ProducerRecord<String, String>(topic, infoItem.getName(), infoItem.toString()), new KafsProducerCallback());

            if(syncSend) recordMetadataFuture.get();

            sleep((long) (Math.random() * 1000));
        } while (true);
    }

    public static InfoItem createInfoItem(int i){

        return new InfoItem(i, names.get(i % names.size()), Timestamp.valueOf(LocalDateTime.now()), Math.random() * i);
    }

}

package com.koko.kafs.conf;

import com.koko.kafs.producer.KafsPartitioner;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.PropertySource;

import java.util.Properties;

@Configuration
@PropertySource("classpath:application.properties")
public class KafsConfig {
    @Bean
    public KafkaProducer<String, String> kafkaProducer(){
        try {
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("client.id", "kafs-producer");
            props.put("acks", "all");
            props.put("retries", "3");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, KafsPartitioner.class);
            return new KafkaProducer<String, String>(props);
        } catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }

    @Bean
    public KafkaConsumer<String, String> kafkaConsumer(){
        try{
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("group.id","kafs-gr");
            props.put("auto.offset.reset","earliest");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            KafkaConsumer<String, String> kk = new KafkaConsumer<String, String>(props);
            return kk;
        }catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }

}

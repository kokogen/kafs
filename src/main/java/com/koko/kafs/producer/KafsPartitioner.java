package com.koko.kafs.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class KafsPartitioner implements Partitioner {

    @Value("${specialName}")
    String specialName;

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int numPartitions = cluster.partitionsForTopic(topic).size();

        if(!(key instanceof String theKey)) return 0;

        if((theKey.equals(specialName))) return numPartitions - 1;

        return Math.abs(Utils.murmur2(keyBytes)) % (numPartitions - 1);
    }

    @Override
    public void close() {
        System.out.println("KafsPartitioner.close()");
    }

    @Override
    public void configure(Map<String, ?> map) {
        if (map.containsKey("specialName"))
            specialName = map.get("specialName").toString();
    }
}

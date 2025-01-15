package sbp.school.kafka.partitioner;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

/**
 * Реализация Partitioner для определения партиции, в зависимости от ключа.
 * Параметр Kafka: partitioner.class
 */

@Slf4j
public class TransactionPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        if (keyBytes == null) {
            log.debug("Key is null. Returning default partition 0");
            return 0;
        }
        int hash = key.hashCode();
        int partition = Math.abs(hash % numPartitions);
        log.debug("key = {}, keyBytes = {}, partition = {}", key, keyBytes, partition);
        return partition;
    }

    @Override
    public void close() {
        log.info("Partitioner closing.");
    }

    @Override
    public void configure(Map<String, ?> configs) {
        log.info("Configuration: configs = {}", configs);
    }
}

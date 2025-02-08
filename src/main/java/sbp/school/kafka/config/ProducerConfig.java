package sbp.school.kafka.config;

import lombok.extern.slf4j.Slf4j;
import sbp.school.kafka.util.PropertiesUtil;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.PARTITIONER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

/**
 * Конфигурационный класс для получения параметров конфигурации Kafka#Producer
 */

@Slf4j
public class ProducerConfig {
    private static final Properties properties = new Properties();

    static {
        loadProducerProperties();
    }

    public static Properties getProducerProperties() {
        return properties;
    }
    private static void loadProducerProperties() {
        log.info("Init ProducerConfig");

        properties.put(BOOTSTRAP_SERVERS_CONFIG, PropertiesUtil.get(BOOTSTRAP_SERVERS_CONFIG));
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, PropertiesUtil.get(KEY_SERIALIZER_CLASS_CONFIG));
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, PropertiesUtil.get(VALUE_SERIALIZER_CLASS_CONFIG));
        properties.put(ACKS_CONFIG, PropertiesUtil.get(ACKS_CONFIG));
        properties.put(PARTITIONER_CLASS_CONFIG, PropertiesUtil.get(PARTITIONER_CLASS_CONFIG));
    }
}

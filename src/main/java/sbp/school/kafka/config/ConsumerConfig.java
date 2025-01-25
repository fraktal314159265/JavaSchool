package sbp.school.kafka.config;

import lombok.extern.slf4j.Slf4j;
import sbp.school.kafka.util.PropertiesUtil;

import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

/**
 * Конфигурационный класс для получения параметров конфигурации Kafka#Consumer
 */
@Slf4j
public class ConsumerConfig {
    private static final Properties properties = new Properties();

    static {
        loadConsumerProperties();
    }

    public static Properties getConsumerProperties() {
        return properties;
    }

    private static void loadConsumerProperties() {
        log.info("Init ProducerConfig");

        properties.put(BOOTSTRAP_SERVERS_CONFIG, PropertiesUtil.get(BOOTSTRAP_SERVERS_CONFIG));
        properties.put(GROUP_ID_CONFIG, PropertiesUtil.get(GROUP_ID_CONFIG));
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, PropertiesUtil.get(KEY_DESERIALIZER_CLASS_CONFIG));
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, PropertiesUtil.get(VALUE_DESERIALIZER_CLASS_CONFIG));
        properties.put(ENABLE_AUTO_COMMIT_CONFIG, PropertiesUtil.get(ENABLE_AUTO_COMMIT_CONFIG));
    }
}

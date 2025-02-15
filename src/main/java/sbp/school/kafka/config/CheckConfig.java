package sbp.school.kafka.config;

import lombok.extern.slf4j.Slf4j;
import sbp.school.kafka.util.PropertiesUtil;

import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

/**
 * Конфигурационный класс для получения параметров конфигурации Kafka#Check
 */
@Slf4j
public class CheckConfig {
    private static final String CHECK = "check.";
    private static final Properties properties = new Properties();

    static {
        loadCheckProperties();
    }

    public static Properties getCheckProperties() {
        return properties;
    }

    private static void loadCheckProperties() {
        log.info("Init CheckConfig");

        properties.put(BOOTSTRAP_SERVERS_CONFIG, PropertiesUtil.get(BOOTSTRAP_SERVERS_CONFIG));
        properties.put(GROUP_ID_CONFIG, PropertiesUtil.get(CHECK + GROUP_ID_CONFIG));
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, PropertiesUtil.get(CHECK + KEY_DESERIALIZER_CLASS_CONFIG));
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, PropertiesUtil.get(CHECK + VALUE_DESERIALIZER_CLASS_CONFIG));
    }
}

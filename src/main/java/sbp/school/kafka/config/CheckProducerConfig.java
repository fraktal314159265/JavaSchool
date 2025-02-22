package sbp.school.kafka.config;

import lombok.extern.slf4j.Slf4j;
import sbp.school.kafka.util.PropertiesUtil;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

@Slf4j
public class CheckProducerConfig {
    private static final Properties properties = new Properties();
    private static final String CHECK = "check.";

    static {
        loadProducerProperties();
    }

    public static Properties getProducerProperties() {
        return properties;
    }
    private static void loadProducerProperties() {
        log.info("Init CheckProducerConfig");

        properties.put(BOOTSTRAP_SERVERS_CONFIG, PropertiesUtil.get(BOOTSTRAP_SERVERS_CONFIG));
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, PropertiesUtil.get(CHECK + KEY_SERIALIZER_CLASS_CONFIG));
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, PropertiesUtil.get(CHECK + VALUE_SERIALIZER_CLASS_CONFIG));
        properties.put(ACKS_CONFIG, PropertiesUtil.get(ACKS_CONFIG));
    }
}

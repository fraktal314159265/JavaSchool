package sbp.school.kafka.util;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Properties;

/**
 * Утилитный класс для чтения параметров из конфигурационного файла application.properties.
 */

@UtilityClass
@Slf4j
public class PropertiesUtil {
    public static final String TOPICS_DEMO_PROPERTY = "topic.demo";
    public static final String TOPIC_CHECK_PROPERTY = "check.topic";
    public static final String INTERVAL = "interval";
    public static final String DELAY = "delay";
    public static final String BATCH_SIZE_PROPERTY = "batch.size";
    public static final String DB_URL = "db.url";
    public static final String DB_USER = "db.user";
    public static final String DB_PASSWORD = "db.password";
    public static final String TABLE_NAME = "table.name";
    public static final String TOPIC_CONFIG = "topic.config";
    private static final Properties PROPERTIES = new Properties();

    static {
        loadProperties();
    }

    public static String get(String key) {
        return PROPERTIES.getProperty(key);
    }

    private static void loadProperties() {
        log.info("Load properties");
        try (var inputStream = PropertiesUtil.class.getClassLoader().getResourceAsStream("application.properties")) {
            PROPERTIES.load(inputStream);
            System.out.println(PROPERTIES);
        } catch (IOException e) {
            log.error("Error load properties", e);
            throw new RuntimeException(e);
        }
    }
}

package sbp.school.kafka.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static sbp.school.kafka.util.PropertiesUtil.DB_PASSWORD;
import static sbp.school.kafka.util.PropertiesUtil.DB_URL;
import static sbp.school.kafka.util.PropertiesUtil.DB_USER;
import static sbp.school.kafka.util.PropertiesUtil.TABLE_NAME;
import static sbp.school.kafka.util.PropertiesUtil.TOPIC_CONFIG;

@Slf4j
public class JdbcSourceConnector extends SourceConnector {

    static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(DB_URL, Type.STRING, Importance.HIGH, "Database URL")
            .define(DB_USER, Type.STRING, Importance.HIGH, "Database username")
            .define(DB_PASSWORD, Type.STRING, Importance.HIGH, "Database password")
            .define(TABLE_NAME, Type.STRING, Importance.HIGH, "Source table name")
            .define(TOPIC_CONFIG, Type.STRING, Importance.HIGH, "Target Kafka topic");

    private Map<String, String> props;

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
        log.info("Starting JDBC Source Connector");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return JdbcSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return Collections.nCopies(maxTasks, props);
    }

    @Override
    public void stop() {
        log.info("Stopping JDBC Source Connector");
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public String version() {
        return "1.0";
    }
}

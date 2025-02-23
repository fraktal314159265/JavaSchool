package sbp.school.kafka.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static sbp.school.kafka.util.PropertiesUtil.DB_PASSWORD;
import static sbp.school.kafka.util.PropertiesUtil.DB_URL;
import static sbp.school.kafka.util.PropertiesUtil.DB_USER;
import static sbp.school.kafka.util.PropertiesUtil.TABLE_NAME;
import static sbp.school.kafka.util.PropertiesUtil.TOPIC_CONFIG;

@Slf4j
public class JdbcSourceTask extends SourceTask {
    private String dbUrl;
    private String dbUser;
    private String dbPassword;
    private String tableName;
    private String topic;
    private Connection connection;

    @Override
    public void start(Map<String, String> props) {
        dbUrl = props.get(DB_URL);
        dbUser = props.get(DB_USER);
        dbPassword = props.get(DB_PASSWORD);
        tableName = props.get(TABLE_NAME);
        topic = props.get(TOPIC_CONFIG);

        try {
            connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
            log.info("Connected to database");
        } catch (SQLException e) {
            throw new ConnectException("Failed to connect to database", e);
        }
    }

    @Override
    public List<SourceRecord> poll() {
        List<SourceRecord> records = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            while (rs.next()) {
                String value = rs.getString("value");
                records.add(new SourceRecord(null, null, topic, null, null, null, value));
                log.debug("Read record: {}", value);
            }
        } catch (SQLException e) {
            log.error("Failed to read records", e);
        }
        return records;
    }

    @Override
    public void stop() {
        try {
            if (connection != null) {
                connection.close();
                log.info("Closed database connection");
            }
        } catch (SQLException e) {
            log.error("Failed to close database connection", e);
        }
    }

    @Override
    public String version() {
        return "1.0";
    }
}

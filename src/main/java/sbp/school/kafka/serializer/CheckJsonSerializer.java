package sbp.school.kafka.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;
import sbp.school.kafka.entity.Check;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Сериализатор для объекта Check.
 * Параметр Kafka: value.serializer
 */

@Slf4j
public class CheckJsonSerializer implements Serializer<Check> {
    @Override
    public byte[] serialize(String topic, Check check) {
        log.debug("serialize Check");
        try {
            if (Objects.nonNull(check)) {
                ObjectMapper objectMapper = new ObjectMapper();
                objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
                objectMapper.registerModule(new JavaTimeModule());

                return objectMapper.writeValueAsString(check).getBytes(StandardCharsets.UTF_8);
            }
        } catch (JsonProcessingException e) {
            log.error("Error serialize", e);
            throw new RuntimeException(e);
        }
        return new byte[0];
    }
}

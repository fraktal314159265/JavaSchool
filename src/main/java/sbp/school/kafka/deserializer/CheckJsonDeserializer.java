package sbp.school.kafka.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import sbp.school.kafka.entity.Check;

import java.io.IOException;
import java.util.Objects;

@Slf4j
public class CheckJsonDeserializer implements Deserializer<Check> {
    @Override
    public Check deserialize(String s, byte[] bytes) {
        log.debug("deserialize Check");
        try {
            if (Objects.nonNull(bytes)) {
                ObjectMapper objectMapper = new ObjectMapper();
                objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
                objectMapper.registerModule(new JavaTimeModule());

                return objectMapper.readValue(bytes, Check.class);
            }
        } catch (IOException e) {
            log.error("Error deserialize Check", e);
            throw new RuntimeException(e);
        }

        return new Check();
    }
}

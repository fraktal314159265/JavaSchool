package sbp.school.kafka.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;
import sbp.school.kafka.entity.Transaction;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Сериализатор для объекта Transaction.
 * Параметр Kafka: value.serializer
 */

@Slf4j
public class TransactionJsonSerializer implements Serializer<Transaction> {
    @Override
    public byte[] serialize(String topic, Transaction transaction) {
        log.debug("serialize Transaction");
        try {
            if (Objects.nonNull(transaction)) {
                ObjectMapper objectMapper = new ObjectMapper();
                objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
                objectMapper.registerModule(new JavaTimeModule());

                return objectMapper.writeValueAsString(transaction).getBytes(StandardCharsets.UTF_8);
            }
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return null;
    }
}

package sbp.school.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.entity.Transaction;

import java.util.Objects;

/**
 * Класс отвечающий за отправку сообщений.
 */

@Slf4j
public class ProducerService {
    public ProducerService() {

    }

    public void send(Transaction transaction, String topic) {
        log.info("Send transaction. Topic name: {}", topic);
        try (KafkaProducer<String, Transaction> producer = new KafkaProducer<>(KafkaConfig.getKafkaProperties())) {
            producer.send(new ProducerRecord<>(topic, transaction.getOperationType().name(), transaction),
                    ((recordMetadata, e) -> {
                        if (Objects.nonNull(e)) {
                            log.error("Offset: {}, Partition: {}", recordMetadata.offset(), recordMetadata.partition());
                            throw new RuntimeException(e);
                        }
                    }));

            producer.flush();
        }
    }
}


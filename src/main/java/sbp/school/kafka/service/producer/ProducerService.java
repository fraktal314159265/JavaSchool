package sbp.school.kafka.service.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import sbp.school.kafka.entity.Transaction;

import java.util.Objects;

/**
 * Класс отвечающий за отправку сообщений.
 */

@Slf4j
public class ProducerService {
    private Producer<String, Transaction> producer;

    public ProducerService(Producer<String, Transaction> producer) {
        this.producer = producer;
    }

    public void send(Transaction transaction, String topic) {
        log.info("Send transaction. Topic name: {}", topic);
        try {
            producer.send(new ProducerRecord<>(topic, transaction.getOperationType().name(), transaction),
                    ((recordMetadata, e) -> {
                        if (Objects.nonNull(e)) {
                            log.error("Offset: {}, Partition: {}", recordMetadata.offset(), recordMetadata.partition());
                            throw new RuntimeException(e);
                        }
                    }));

            producer.flush();
        } finally {
            producer.close();
        }
    }
}


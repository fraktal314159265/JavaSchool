package sbp.school.kafka.service.checking;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import sbp.school.kafka.config.CheckConfig;
import sbp.school.kafka.entity.Check;
import sbp.school.kafka.entity.Transaction;
import sbp.school.kafka.repository.TransactionRepository;
import sbp.school.kafka.service.producer.ProducerService;
import sbp.school.kafka.util.PropertiesUtil;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;

import static sbp.school.kafka.util.PropertiesUtil.DELAY;
import static sbp.school.kafka.util.PropertiesUtil.INTERVAL;
import static sbp.school.kafka.util.PropertiesUtil.TOPICS_DEMO_PROPERTY;
import static sbp.school.kafka.util.PropertiesUtil.TOPIC_CHECK_PROPERTY;

@Slf4j
@RequiredArgsConstructor
public class CheckService {

    private final TransactionRepository transactionRepository;
    private final ProducerService producerService;

    /**
     * Читает топик, в который consumer#ConsumerService кладёт время обработки транзакций и их хеш-сумму.
     * Параметр interval определяет, за сколько минут были обработаны сообщения
     * Параметр delay определяет возможную задержку
     *
     */
    public void read() {
        try (KafkaConsumer<String, Check> consumer = new KafkaConsumer<>(CheckConfig.getCheckProperties())) {
            consumer.subscribe(Collections.singleton(PropertiesUtil.get(TOPIC_CHECK_PROPERTY)));

            while (true) {
                ConsumerRecords<String, Check> records = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<String, Check> record : records) {
                    String topic = record.topic();
                    long offset = record.offset();
                    int partition = record.partition();
                    String key = record.key();
                    Check message = record.value();
                    log.info("Topic: {}, Offset:{}, Partition: {}, Key: {}, Message: {}",
                            topic,
                            offset,
                            partition,
                            key,
                            message
                    );
                    check(message);
                }
            }
        }
    }

    private void check(Check check) {
        OffsetDateTime dateTime = check.getOffsetDateTime();
        int hashSumma = check.getHashSumma();

        List<Transaction> transactionList = transactionRepository.findTransactionByInterval(
                dateTime,
                Long.parseLong(PropertiesUtil.get(INTERVAL)),
                Long.parseLong(PropertiesUtil.get(DELAY)));

        int checkHash = transactionList.stream()
                .map(transaction -> transaction.getId().hashCode())
                .reduce(0, Integer::sum);

        log.info("checkHash: {}", checkHash);

        if (hashSumma != checkHash) {
            transactionList.forEach(
                    transaction -> producerService.send(transaction, PropertiesUtil.get(TOPICS_DEMO_PROPERTY))
            );
        }
    }

}

package sbp.school.kafka.service.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import sbp.school.kafka.config.ConsumerConfig;
import sbp.school.kafka.entity.ProcessedTransaction;
import sbp.school.kafka.entity.Transaction;
import sbp.school.kafka.repository.ProcessedTransactionRepository;
import sbp.school.kafka.util.PropertiesUtil;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static sbp.school.kafka.util.PropertiesUtil.BATCH_SIZE_PROPERTY;
import static sbp.school.kafka.util.PropertiesUtil.TOPICS_DEMO_PROPERTY;

@Slf4j
@RequiredArgsConstructor
public class ConsumerService {
    private final Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();
    private long counter = 0L;
    private final ProcessedTransactionRepository processedTransactionRepository;

    /**
     * При первом вызове метода pool, он отвечает за поиск координатора группы, за присоединение потребителя к группе,
     * и назначение ему партиции.
     * Метод pool посылает heartbeats (сигналы для Kafka, показывающие, что приложение работает).
     * Важно, чтобы метод pool выполнялся регулярно, чтобы не было перебалансировок. Если при обработке сообщений
     * необходимо выполнить долгие операции (например, обращение к стороннему приложению), то такие операции лучше
     * делать в отдельном потоке, чтобы регулярность отправки heartbeats не нарушалась.
     */
    public void read() {
        KafkaConsumer<String, Transaction> consumer = null;
        String topic = null;
        long offset = -1;
        int partition = -1;
        String key = null;
        Transaction message = null;
        try {
            consumer = new KafkaConsumer<>(ConsumerConfig.getConsumerProperties());
            consumer.subscribe(getTopics());

            while (true) {
                ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<String, Transaction> record : records) {
                    topic = record.topic();
                    offset = record.offset();
                    partition = record.partition();
                    key = record.key();
                    message = record.value();
                    log.info("Topic: {}, Offset:{}, Partition: {}, Key: {}, Message: {}",
                            topic,
                            offset,
                            partition,
                            key,
                            message
                    );
                    processedTransactionRepository.save(ProcessedTransaction.builder()
                            .transaction(message)
                            .build());

                    currentOffset.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset + 1));

                    int batch = Integer.parseInt(PropertiesUtil.get(BATCH_SIZE_PROPERTY));
                    if (counter % batch == 0) {
                        consumer.commitAsync(currentOffset, null);
                    }
                    counter++;
                }
            }
        } catch (Exception e) {
            log.error("Consumer Error – Topic: {}, Offset:{}, Partition: {}, Key: {}, Message: {}",
                    topic,
                    offset,
                    partition,
                    key,
                    message);
        } finally {
            try {
                Optional.ofNullable(consumer).ifPresent(it -> it.commitSync(currentOffset));
            } finally {
                Optional.ofNullable(consumer).ifPresent(KafkaConsumer::close);
            }
        }
    }


    private List<String> getTopics() {
        return Arrays.stream(PropertiesUtil.get(TOPICS_DEMO_PROPERTY).split(";"))
                .map(String::trim)
                .collect(Collectors.toList());
    }
}

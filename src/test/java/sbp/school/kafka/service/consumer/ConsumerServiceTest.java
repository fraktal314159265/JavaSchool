package sbp.school.kafka.service.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sbp.school.kafka.entity.OperationType;
import sbp.school.kafka.entity.ProcessedTransaction;
import sbp.school.kafka.entity.Transaction;
import sbp.school.kafka.repository.ProcessedTransactionRepository;

import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsumerServiceTest {
    private ConsumerService consumerService;
    private ProcessedTransactionRepository repository;
    private MockConsumer<String, Transaction> mockConsumer;

    @BeforeEach
    void setUp() {
        repository = new ProcessedTransactionRepository();
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumerService = new ConsumerService(repository, mockConsumer);
    }

    @Test
    void testReadAndProcessMessages() throws InterruptedException {
        String topic = "test-topic";
        int partition = 0;
        long offset = 0;
        String key = "key";
        Transaction transaction = new Transaction(1L, OperationType.PURCHASE, 1.2, "1-2", OffsetDateTime.now());

        // Создаем тестовое сообщение
        ConsumerRecord<String, Transaction> record = new ConsumerRecord<>(topic, partition, offset, key, transaction);

        // Назначаем партицию и топик
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        mockConsumer.assign(Collections.singletonList(topicPartition));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(topicPartition, 0L));

        // Добавляем сообщение в MockConsumer
        mockConsumer.addRecord(record);

        //Запускаем метод read() в отдельном потоке
        Thread consumerThread = new Thread(() -> consumerService.read());
        consumerThread.start();

        // Ждем, пока сообщение будет обработано
        Thread.sleep(2000);

        // Проверяем, что сообщение было обработано и сохранено
        List<ProcessedTransaction> processedTransactions = repository.getProcessedTransactions();
        assertEquals(1, processedTransactions.size(), "Сообщение не было обработано");
        assertEquals(transaction, processedTransactions.get(0).getTransaction(), "Сообщение не совпадает");

        // Проверяем, что смещение было обновлено
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = mockConsumer.committed(Collections.singleton(topicPartition));
        assertNotNull(committedOffsets.get(topicPartition), "Смещение не было обновлено");
        assertEquals(offset + 1, committedOffsets.get(topicPartition).offset(), "Некорректное смещение");
    }

    @Test
    void testReadWithException() throws InterruptedException {
        // Настройка MockConsumer для выброса исключения
        mockConsumer.schedulePollTask(() -> {
            throw new RuntimeException("Test exception");
        });

        // Запускаем метод read() в отдельном потоке
        Thread consumerThread = new Thread(() -> consumerService.read());
        consumerThread.start();

        // Ждем, пока поток завершится
        consumerThread.join(); // Ждем завершения потока

        // Проверяем, что consumer был закрыт
        assertTrue(mockConsumer.closed(), "Consumer не был закрыт");
    }
}

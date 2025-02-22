package sbp.school.kafka.service.producer;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sbp.school.kafka.entity.OperationType;
import sbp.school.kafka.entity.Transaction;
import sbp.school.kafka.serializer.TransactionJsonSerializer;

import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProducerServiceTest {

    private MockProducer<String, Transaction> mockProducer;
    private ProducerService producerService;

    @BeforeEach
    void setUp() {
        // Создаем MockProducer с сериализаторами
        mockProducer = new MockProducer<>(true, new StringSerializer(), new TransactionJsonSerializer());
        producerService = new ProducerService(mockProducer);
    }

    @Test
    void testSendSuccess() {
        // Подготовка данных
        String topic = "test-topic";
        Transaction transaction = new Transaction(1L, OperationType.PURCHASE, 1.2, "1-2", OffsetDateTime.now());

        // Вызов метода send
        producerService.send(transaction, topic);

        // Проверяем, что сообщение было отправлено
        assertEquals(1, mockProducer.history().size(), "Сообщение не было отправлено");

        // Проверяем содержимое отправленного сообщения
        ProducerRecord<String, Transaction> record = mockProducer.history().get(0);
        assertEquals(topic, record.topic(), "Некорректный топик");
        assertEquals(transaction.getOperationType().name(), record.key(), "Некорректный ключ");
        assertEquals(transaction, record.value(), "Некорректное значение");

        // Проверяем, что producer был закрыт
        assertTrue(mockProducer.closed(), "Producer не был закрыт");
    }
}
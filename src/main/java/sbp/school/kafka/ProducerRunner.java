package sbp.school.kafka;

import sbp.school.kafka.entity.OperationType;
import sbp.school.kafka.entity.Transaction;
import sbp.school.kafka.service.ProducerService;

import java.time.OffsetDateTime;

public class ProducerRunner {
    private static final String TOPIC = "sberbank-kafka-demo";
    public static void main(String[] args) {

        ProducerService producerService = new ProducerService();

        Transaction transaction1 = Transaction.builder()
                .operationType(OperationType.SALE)
                .amount(1.00)
                .date(OffsetDateTime.now())
                .account("SB0001")
                .build();

        Transaction transaction2 = Transaction.builder()
                .operationType(OperationType.CHARITY)
                .amount(2.00)
                .date(OffsetDateTime.now())
                .account("SB0002")
                .build();

        Transaction transaction3 = Transaction.builder()
                .operationType(OperationType.PURCHASE)
                .amount(3.00)
                .date(OffsetDateTime.now())
                .account("SB0003")
                .build();

        producerService.send(transaction1, TOPIC);
        producerService.send(transaction2, TOPIC);
        producerService.send(transaction3, TOPIC);
    }
}

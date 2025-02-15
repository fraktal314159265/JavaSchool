package sbp.school.kafka.repository;

import lombok.extern.slf4j.Slf4j;
import sbp.school.kafka.entity.Transaction;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class TransactionRepository {
    private static final List<Transaction> TRANSACTION_DB = new ArrayList<>();

    public List<Transaction> findTransactionByInterval(OffsetDateTime offsetDateTime, long interval, long delay) {
        OffsetDateTime start = offsetDateTime.minusSeconds(interval).withOffsetSameInstant(offsetDateTime.getOffset());
        OffsetDateTime end = offsetDateTime.plusSeconds(delay).withOffsetSameInstant(offsetDateTime.getOffset());
        return TRANSACTION_DB.stream()
                .filter(transaction -> transaction.getDate().isAfter(start) && transaction.getDate().isBefore(end))
                .collect(Collectors.toList());
    }
    public void saveTransaction(Transaction transaction) {
        TRANSACTION_DB.add(transaction);
    }

    public List<Transaction> findAllTransaction() {
        return TRANSACTION_DB;
    }
}

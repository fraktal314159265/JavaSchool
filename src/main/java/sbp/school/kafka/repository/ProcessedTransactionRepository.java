package sbp.school.kafka.repository;

import lombok.extern.slf4j.Slf4j;
import sbp.school.kafka.entity.ProcessedTransaction;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class ProcessedTransactionRepository {
    private static final List<ProcessedTransaction> PROCESSED_TRANSACTIONS_DB = new ArrayList<>();

    public List<ProcessedTransaction> findProcessedTransactionByInterval(OffsetDateTime offsetDateTime, long interval, long delay) {
        OffsetDateTime start = offsetDateTime.minusSeconds(delay).withOffsetSameInstant(offsetDateTime.getOffset());
        OffsetDateTime end = offsetDateTime.plusSeconds(interval).withOffsetSameInstant(offsetDateTime.getOffset());
        return PROCESSED_TRANSACTIONS_DB.stream()
                .filter(transaction -> transaction.getDate().isAfter(start) && transaction.getDate().isBefore(end))
                .collect(Collectors.toList());
    }

    public void save(ProcessedTransaction processedTransaction) {
        PROCESSED_TRANSACTIONS_DB.add(processedTransaction);
    }
}

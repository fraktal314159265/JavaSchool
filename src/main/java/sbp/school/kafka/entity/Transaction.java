package sbp.school.kafka.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.OffsetDateTime;

/**
 * Модель Операция.
 */

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Getter
@Setter
public class Transaction {
    private OperationType operationType;
    private Double amount;
    private String account;
    private OffsetDateTime date;
}

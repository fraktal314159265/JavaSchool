package sbp.school.kafka.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.time.OffsetDateTime;

/**
 * Обёртка для Transaction, необходимая для фиксирования времени обработки поступившей транзакции.
 */

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Getter
@Setter
@ToString
public class ProcessedTransaction {
    private Transaction transaction;
    @Builder.Default
    private OffsetDateTime date = OffsetDateTime.now();
}

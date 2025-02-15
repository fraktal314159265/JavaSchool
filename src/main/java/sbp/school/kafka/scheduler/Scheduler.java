package sbp.school.kafka.scheduler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import sbp.school.kafka.config.CheckProducerConfig;
import sbp.school.kafka.entity.Check;
import sbp.school.kafka.repository.ProcessedTransactionRepository;
import sbp.school.kafka.util.PropertiesUtil;

import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static sbp.school.kafka.util.PropertiesUtil.DELAY;
import static sbp.school.kafka.util.PropertiesUtil.INTERVAL;
import static sbp.school.kafka.util.PropertiesUtil.TOPIC_CHECK_PROPERTY;

@Slf4j
public class Scheduler {
    private static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private static ProcessedTransactionRepository repository = new ProcessedTransactionRepository();

    static {
        execute();
    }

    public static void execute() {
        Runnable task = () -> {
            try (KafkaProducer<String, Check> producer = new KafkaProducer<>(CheckProducerConfig.getProducerProperties())) {
                OffsetDateTime now = OffsetDateTime.now();
                int hashSumma = getHashSumma(now);

                Check check = Check.builder()
                        .offsetDateTime(now)
                        .hashSumma(hashSumma)
                        .build();

                producer.send(new ProducerRecord<>(PropertiesUtil.get(TOPIC_CHECK_PROPERTY), check), ((recordMetadata, e) -> {
                    if (Objects.nonNull(e)) {
                        log.error("Offset: {}, Partition: {}", recordMetadata.offset(), recordMetadata.partition());
                        throw new RuntimeException(e);
                    }
                }));

            }
        };

        scheduler.scheduleAtFixedRate(task, 0, Long.parseLong(PropertiesUtil.get(INTERVAL)), TimeUnit.SECONDS);
    }

    private static int getHashSumma(OffsetDateTime now) {
        return repository.findProcessedTransactionByInterval(
                        now,
                        Long.parseLong(PropertiesUtil.get(INTERVAL)),
                        Long.parseLong(PropertiesUtil.get(DELAY)))
                .stream()
                .map(it -> it.getTransaction().getId().hashCode())
                .reduce(0, Integer::sum);
    }

}

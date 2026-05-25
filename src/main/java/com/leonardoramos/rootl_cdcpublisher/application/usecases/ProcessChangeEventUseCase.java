package com.leonardoramos.rootl_cdcpublisher.application.usecases;

import com.leonardoramos.rootl_cdcpublisher.application.ports.outbound.EventPublisherPort;
import com.leonardoramos.rootl_cdcpublisher.application.ports.outbound.OffsetStorePort;
import com.leonardoramos.rootl_cdcpublisher.domain.model.ChangeEvent;
import com.leonardoramos.rootl_cdcpublisher.domain.services.TransactionBuffer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public class ProcessChangeEventUseCase {

    private static final Logger log = LoggerFactory.getLogger(ProcessChangeEventUseCase.class);

    private final EventPublisherPort publisher;
    private final OffsetStorePort offsetStore;
    private final TransactionBuffer transactionBuffer;
    private final String connectorId;
    private final MeterRegistry registry;

    public ProcessChangeEventUseCase(EventPublisherPort publisher,
                                     OffsetStorePort offsetStore,
                                     String connectorId,
                                     MeterRegistry registry) {
        this.publisher = publisher;
        this.offsetStore = offsetStore;
        this.connectorId = connectorId;
        this.registry = registry;
        this.transactionBuffer = new TransactionBuffer();

        Gauge.builder("cdc.transaction.buffer.size", transactionBuffer, tb -> tb.getTotalEventsCount())
                .tag("connector", connectorId)
                .description("Número total de eventos retidos no buffer aguardando COMMIT")
                .register(registry);
    }

    public void process(ChangeEvent event) {
        String txId = event.source().transactionId();

        if (event.timestamp() != null) {
            long lagMs = Instant.now().toEpochMilli() - event.timestamp().toEpochMilli();
            Timer.builder("cdc.replication.lag")
                    .tag("connector", connectorId)
                    .description("Atraso de replicação em milissegundos")
                    .register(registry)
                    .record(Duration.ofMillis(Math.max(0, lagMs)));
        }

        if (event.source().table() != null && !"system".equals(event.source().schema())) {
            Counter.builder("cdc.events.processed")
                    .tag("connector", connectorId)
                    .tag("schema", event.source().schema())
                    .tag("table", event.source().table())
                    .tag("operation", event.operation().name())
                    .description("Contagem de eventos processados")
                    .register(registry)
                    .increment();
        }

        switch (event.operation()) {
            case BEGIN -> log.debug("Iniciando buffer para transação: {}", txId);
            case INSERT, UPDATE, DELETE, READ -> transactionBuffer.addEvent(txId, event);
            case COMMIT -> flushTransaction(txId, event.source().offsetCoordinates());
        }
    }

    private void flushTransaction(String transactionId, Map<String, String> offsetCoordinates) {
        List<ChangeEvent> pendingEvents = transactionBuffer.commit(transactionId);
        if (pendingEvents.isEmpty()) return;

        for (ChangeEvent pendingEvent : pendingEvents) {
            publisher.publish(pendingEvent);
        }

        String offsetToSave = offsetCoordinates.containsKey("lsn") ? offsetCoordinates.get("lsn")
                : offsetCoordinates.containsKey("binlog_pos") ? offsetCoordinates.get("binlog_pos")
                : offsetCoordinates.values().stream().findFirst().orElse("");

        offsetStore.save(connectorId, offsetToSave);
    }
}
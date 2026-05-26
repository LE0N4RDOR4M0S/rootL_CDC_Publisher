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

/**
 * Essa classe é responsável por processar os eventos de mudança recebidos do CDC,
 * gerenciar o buffer de transações para garantir a ordem correta dos eventos dentro de uma transação,
 * e publicar os eventos no destino apropriado (como Kafka) somente após a confirmação
 */
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

        /**
         * Métrica para monitorar o tamanho do buffer de transações
         */
        Gauge.builder("cdc.transaction.buffer.size", transactionBuffer, tb -> tb.getTotalEventsCount())
                .tag("connector", connectorId)
                .description("Número total de eventos retidos no buffer aguardando COMMIT")
                .register(registry);
    }

    /**
     * Processa um evento de mudança, gerencindo o buffer de transações e publicando os eventos somente após
     * a confirmação da transação (COMMIT). Para eventos de início de transação (BEGIN), o evento é adicionado ao buffer.
     * Para eventos de modificação (INSERT, UPDATE, DELETE, READ), o evento é adicionado ao buffer associado à transação.
     * Para eventos de confirmação (COMMIT), os eventos pendentes no buffer são publicados e o offset é salvo.
     * @param event O evento de mudança a ser processado.
     */
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

    /**
     * Publica os eventos pendentes no buffer para a transação especificada e salva o offset após a publicação.
     * Os eventos são publicados na ordem em que foram recebidos, garantindo a consistência dos dados.
     * O offset salvo é determinado pelas coordenadas de offset fornecidas, priorizando "lsn" ou "binlog_pos" se disponíveis,
     * ou usando o primeiro valor disponível como fallback.
     * @param transactionId O identificador da transação cujos eventos pendentes devem ser publicados.
     * @param offsetCoordinates As coordenadas de offset associadas à transação, utilizadas para determinar o ponto de salvamento do offset após a publicação dos eventos.
     */
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
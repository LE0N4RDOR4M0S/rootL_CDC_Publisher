package com.leonardoramos.rootl_cdcpublisher.application.usecases;

import com.leonardoramos.rootl_cdcpublisher.application.ports.outbound.EventPublisherPort;
import com.leonardoramos.rootl_cdcpublisher.application.ports.outbound.OffsetStorePort;
import com.leonardoramos.rootl_cdcpublisher.domain.model.ChangeEvent;
import com.leonardoramos.rootl_cdcpublisher.domain.services.TransactionBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class ProcessChangeEventUseCase {

    private static final Logger log = LoggerFactory.getLogger(ProcessChangeEventUseCase.class);

    private final EventPublisherPort publisher;
    private final OffsetStorePort offsetStore;
    private final TransactionBuffer transactionBuffer;
    private final String connectorId;

    public ProcessChangeEventUseCase(EventPublisherPort publisher,
                                     OffsetStorePort offsetStore,
                                     String connectorId) {
        this.publisher = publisher;
        this.offsetStore = offsetStore;
        this.transactionBuffer = new TransactionBuffer();
        this.connectorId = connectorId;
    }

    public void process(ChangeEvent event) {
        String txId = event.source().transactionId();

        switch (event.operation()) {
            case BEGIN -> {
                log.debug("Iniciando buffer para transação: {}", txId);
            }
            case INSERT, UPDATE, DELETE, READ -> {
                transactionBuffer.addEvent(txId, event);
            }
            case COMMIT -> {
                flushTransaction(txId, event.source().offsetCoordinates());
            }
        }
    }

    private void flushTransaction(String transactionId, Map<String, String> offsetCoordinates) {
        List<ChangeEvent> pendingEvents = transactionBuffer.commit(transactionId);

        if (pendingEvents.isEmpty()) {
            return;
        }

        for (ChangeEvent pendingEvent : pendingEvents) {
            publisher.publish(pendingEvent);
        }

        String offsetToSave = offsetCoordinates.containsKey("lsn")
                ? offsetCoordinates.get("lsn")
                : offsetCoordinates.toString();

        offsetStore.save(connectorId, offsetToSave);

        log.info("Transação {} ({} eventos) processada e offset [{}] salvo com sucesso.",
                transactionId, pendingEvents.size(), offsetToSave);
    }
}
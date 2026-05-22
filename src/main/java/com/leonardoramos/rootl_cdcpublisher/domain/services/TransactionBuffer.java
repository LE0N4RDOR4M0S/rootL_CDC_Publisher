package com.leonardoramos.rootl_cdcpublisher.domain.services;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.leonardoramos.rootl_cdcpublisher.domain.model.ChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionBuffer {

    private static final Logger log = LoggerFactory.getLogger(TransactionBuffer.class);
    private static final int MAX_EVENTS_PER_TX = 10_000;

    private final Map<String, List<ChangeEvent>> buffer = new ConcurrentHashMap<>();

    public void addEvent(String transactionId, ChangeEvent event) {
        buffer.compute(transactionId, (txId, events) -> {
            if (events == null) {
                events = new ArrayList<>();
            }
            if (events.size() >= MAX_EVENTS_PER_TX) {
                log.warn("Transação {} excedeu o limite do buffer de memória ({}).", txId, MAX_EVENTS_PER_TX);
            } else {
                events.add(event);
            }
            return events;
        });
    }

    public List<ChangeEvent> commit(String transactionId) {
        List<ChangeEvent> events = buffer.remove(transactionId);
        return events != null ? events : List.of();
    }

    public void rollback(String transactionId) {
        buffer.remove(transactionId);
        log.info("Buffer limpo para transação abortada: {}", transactionId);
    }
}

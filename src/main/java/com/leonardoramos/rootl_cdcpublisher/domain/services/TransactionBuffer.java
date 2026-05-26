package com.leonardoramos.rootl_cdcpublisher.domain.services;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.leonardoramos.rootl_cdcpublisher.domain.model.ChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Classe responsável por gerenciar um buffer de eventos de mudança (ChangeEvent)
 */
public class TransactionBuffer {

    private static final Logger log = LoggerFactory.getLogger(TransactionBuffer.class);
    private static final int MAX_EVENTS_PER_TX = 10_000;

    private final Map<String, List<ChangeEvent>> buffer = new ConcurrentHashMap<>();

    /**
     * Adiciona um evento ao buffer associado à transação.
     * @param transactionId identificador da transação à qual o evento pertence.
     * @param event evento de mudança a ser adicionado ao buffer.
     */
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

    /**
     * Recupera e remove os eventos associados à transação, indicando que a transação foi confirmada (COMMIT).
     * @param transactionId identificador da transação a ser confirmada.
     * @return lista de eventos associados à transação.
     */
    public List<ChangeEvent> commit(String transactionId) {
        List<ChangeEvent> events = buffer.remove(transactionId);
        return events != null ? events : List.of();
    }

    /**
     * Remove os eventos associados à transação. (Não usado)
     * @param transactionId identificador da transação a ser abortada.
     */
    public void rollback(String transactionId) {
        buffer.remove(transactionId);
        log.info("Buffer limpo para transação abortada: {}", transactionId);
    }

    /**
     * Retorna o número total de eventos atualmente retidos no buffer, aguardando confirmação (COMMIT).
     * @return número total de eventos no buffer.
     */
    public double getTotalEventsCount() {
        return buffer.values().stream().mapToInt(List::size).sum();
    }
}

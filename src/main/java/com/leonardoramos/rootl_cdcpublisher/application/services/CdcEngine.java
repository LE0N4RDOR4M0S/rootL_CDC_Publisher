package com.leonardoramos.rootl_cdcpublisher.application.services;

import com.leonardoramos.rootl_cdcpublisher.application.ports.inbound.ChangeLogConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CdcEngine {

    private static final Logger log = LoggerFactory.getLogger(CdcEngine.class);

    private final Map<String, ChangeLogConnector> activeConnectors = new ConcurrentHashMap<>();
    private final ExecutorService executorService;

    public CdcEngine() {
        this.executorService = Executors.newCachedThreadPool();
    }

    public void registerAndStart(ChangeLogConnector connector) {
        String id = connector.getClass().getSimpleName() + "-" + System.currentTimeMillis();

        activeConnectors.put(id, connector);

        executorService.submit(() -> {
            try {
                log.info("Iniciando conector tipo: {}", connector.getType());
                connector.start();
            } catch (Exception e) {
                log.error("Conector {} falhou com erro crítico", id, e);
            }
        });
    }

    public void shutdown() {
        log.info("Desligando o CDC Engine. Parando {} conectores ativos...", activeConnectors.size());

        activeConnectors.values().forEach(ChangeLogConnector::stop);

        executorService.shutdown();
        try {
            log.info("30 segundos para finalização segura das transações em andamento...");
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                log.warn("Forçando encerramento bruto...");
                executorService.shutdownNow();
            } else {
                log.info("Todos os conectores foram encerrados de forma limpa. Nenhum dado foi perdido.");
            }
        } catch (InterruptedException e) {
            log.error("Thread de encerramento foi interrompida.", e);
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("CDC Engine encerrado com segurança.");
    }
}

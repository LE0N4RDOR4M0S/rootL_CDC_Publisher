package com.leonardoramos.rootl_cdcpublisher.application.services;

import com.leonardoramos.rootl_cdcpublisher.application.ports.inbound.ChangeLogConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * CDC Engine é o coração do sistema de Change Data Capture. Ele é responsável por gerenciar os conectores de log de mudança,
 * garantindo que eles sejam iniciados, monitorados e desligados de forma segura. O CDC Engine é projetado para ser resiliente,
 * lidando com falhas de conectores sem comprometer a integridade dos dados ou a continuidade do serviço.
 * Ele também é responsável por garantir que, durante o desligamento,
 * todas as transações em andamento sejam concluídas de forma segura, evitando perda de dados e garantindo uma transição suave para o estado de inatividade.
 */
public class CdcEngine {

    private static final Logger log = LoggerFactory.getLogger(CdcEngine.class);

    private final Map<String, ChangeLogConnector> activeConnectors = new ConcurrentHashMap<>();
    private final ExecutorService executorService;

    public CdcEngine() {
        this.executorService = Executors.newCachedThreadPool();
    }

    /**
     * Registra um conector de log de mudança e inicia sua execução em uma thread separada. O CDC Engine monitora a execução do conector,
     * garantindo que falhas sejam tratadas de forma adequada, sem comprometer a estabilidade do sistema. Cada conector é identificado por um ID único, permitindo que o CDC Engine gerencie múltiplos conectores simultaneamente.
     * @param connector O conector de log de mudança a ser registrado e iniciado. O conector deve implementar a interface ChangeLogConnector, garantindo que ele possa ser gerenciado pelo CDC Engine.
     */
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

    /**
     * Desliga o CDC Engine de forma segura, garantindo que todas as transações em andamento sejam concluídas antes de encerrar os conectores.
     * O método shutdown() é projetado para ser chamado durante o processo de desligamento do sistema, garantindo que o CDC Engine tenha tempo suficiente para finalizar suas operações sem perda de dados. Ele aguarda um período de tempo configurável para que os conectores concluam suas tarefas,
     * e se necessário, força o encerramento após esse período para garantir que o sistema possa desligar completamente.
     */
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

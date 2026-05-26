package com.leonardoramos.rootl_cdcpublisher.application.ports.inbound;

import com.leonardoramos.rootl_cdcpublisher.application.ports.outbound.OffsetStorePort;
import com.leonardoramos.rootl_cdcpublisher.application.usecases.ProcessChangeEventUseCase;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.Properties;

/**
 * Interface para o conector de log change. Esta interface define os métodos necessários para inicializar, iniciar e parar o conector de log change, bem como para obter o tipo do conector. O conector de log change é responsável por se conectar ao banco de dados, capturar os eventos de mudança e processá-los utilizando os casos de uso do domínio, além de gerenciar os offsets para garantir um processamento idempotente e seguro.
 */
public interface ChangeLogConnector {

    /**
     * Inicializa o conector de log change com as configurações dos parâmetros.
     * @param connectorId Id do conector, utilizado para logs e métricas
     * @param config Configurações específicas do conector (credenciais, tópicos, etc)
     * @param useCase Casos de uso do domínio para processar os eventos
     * @param offsetStore Armazenamento de offset para garantir processamento idempotente e reinício seguro
     * @param meterRegistry Registro de métricas para monitorar o conector
     */
    void initialize(String connectorId, Properties config, ProcessChangeEventUseCase useCase, OffsetStorePort offsetStore, MeterRegistry meterRegistry);

    /**
     * Inicia o processo de captura e processamento dos eventos de log change.
     */
    void start();

    /**
     * Para o conector, liberando recursos e garantindo que o processo seja finalizado de forma segura.
     */
    void stop();

    /**
     * Retorna o tipo do conector ("oracle", "mysql", "postgresql", etc), utilizado para identificação e métricas.
     * @return
     */
    String getType();
}
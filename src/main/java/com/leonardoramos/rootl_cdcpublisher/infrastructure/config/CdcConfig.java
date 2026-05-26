package com.leonardoramos.rootl_cdcpublisher.infrastructure.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.leonardoramos.rootl_cdcpublisher.adapters.outbound.kafka.KafkaPublisherAdapter;
import com.leonardoramos.rootl_cdcpublisher.adapters.outbound.offsets.FileOffsetStoreAdapter;
import com.leonardoramos.rootl_cdcpublisher.application.ports.inbound.ChangeLogConnector;
import com.leonardoramos.rootl_cdcpublisher.application.ports.outbound.EventPublisherPort;
import com.leonardoramos.rootl_cdcpublisher.application.ports.outbound.OffsetStorePort;
import com.leonardoramos.rootl_cdcpublisher.application.services.CdcEngine;
import com.leonardoramos.rootl_cdcpublisher.application.usecases.ProcessChangeEventUseCase;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Configuração do Spring para o CDC Engine. Esta classe é responsável por configurar os beans necessários para o funcionamento do CDC Engine,
 * incluindo o publisher de eventos, o armazenamento de offsets e a inicialização dos conectores de log change.
 * Os conectores são carregados dinamicamente a partir de arquivos JSON em um diretório configurável,
 * permitindo flexibilidade na adição de novos conectores sem necessidade de recompilar a aplicação.
 */
@Configuration
public class CdcConfig {

    private static final Logger log = LoggerFactory.getLogger(CdcConfig.class);

    @Value("${kafka.bootstrap-servers}")
    private String kafkaBootstrapServers;

    @Value("${cdc.offset-dir:./offsets}")
    private String offsetDir;

    @Value("${cdc.connectors-dir:./connectors}")
    private String connectorsDir;

    /**
     * Configura o bean do EventPublisherPort, que é responsável por publicar os eventos de mudança no Kafka.
     * @return Uma instância do KafkaPublisherAdapter, configurada com os servidores Kafka especificados nas propriedades da aplicação.
     */
    @Bean(destroyMethod = "close")
    public EventPublisherPort eventPublisherPort() {
        return new KafkaPublisherAdapter(kafkaBootstrapServers);
    }

    /**
     * Configura o bean do OffsetStorePort, que é responsável por armazenar os offsets de processamento dos eventos de mudança.
     * @return Uma instância do FileOffsetStoreAdapter, configurada para armazenar os offsets em arquivos no diretório especificado nas propriedades da aplicação.
     */
    @Bean
    public OffsetStorePort offsetStorePort() {
        return new FileOffsetStoreAdapter(offsetDir);
    }

    /**
     * Configura o bean do CdcEngine, que é o coração do sistema de Change Data Capture. O CDC Engine é responsável por gerenciar os conectores de log de mudança, garantindo que eles sejam iniciados, monitorados e desligados de forma segura. Ele é projetado para ser resiliente, lidando com falhas de conectores sem comprometer a integridade dos dados ou a continuidade do serviço. Ele também é responsável por garantir que, durante o desligamento, todas as transações em andamento sejam concluídas de forma segura, evitando perda de dados e garantindo uma transição suave para o estado de inatividade.
     * @return Uma instância do CdcEngine, que será gerenciada pelo Spring e terá seu método shutdown() chamado automaticamente durante o processo de desligamento da aplicação para garantir um encerramento seguro dos conectores.
     */
    @Bean(destroyMethod = "shutdown")
    public CdcEngine cdcEngine() {
        return new CdcEngine();
    }

    /**
     * Configura o ApplicationRunner que é responsável por iniciar o processo de captura de dados (CDC) assim que a aplicação for iniciada. Este runner busca por arquivos de configuração de conectores no diretório especificado, carrega as configurações, inicializa os conectores de log change e os registra no CDC Engine para iniciar a captura dos eventos de mudança. O runner também lida com a criação do diretório de conectores caso ele não exista, e registra logs detalhados sobre o processo de carregamento dos conectores.
     * @param engine O CDC Engine que gerencia os conectores de log change. O runner irá registrar os conectores carregados no CDC Engine para iniciar a captura dos eventos de mudança.
     * @param publisher O EventPublisherPort que é responsável por publicar os eventos de mudança. Ele será injetado nos casos de uso do domínio para que os eventos possam ser publicados no destino apropriado (como Kafka).
     * @param offsetStore O OffsetStorePort que é responsável por armazenar os offsets de processamento dos eventos de mudança. Ele será injetado nos casos de uso do domínio para garantir que os offsets sejam salvos corretamente após o processamento dos eventos.
     * @param meterRegistry O MeterRegistry que é responsável por registrar as métricas do CDC Engine e dos conectores de log change. Ele será injetado nos casos de uso do domínio e nos conectores para que as métricas possam ser registradas e monitoradas adequadamente.
     * @return
     */
    @Bean
    public ApplicationRunner startCdcRunner(CdcEngine engine,
                                            EventPublisherPort publisher,
                                            OffsetStorePort offsetStore,
                                            MeterRegistry meterRegistry) {
        return args -> {
            log.info("Buscando conectores no diretório: {}", connectorsDir);
            Path dirPath = Paths.get(connectorsDir);

            if (!Files.exists(dirPath)) {
                Files.createDirectories(dirPath);
                log.info("Diretório '{}' criado. Adicione arquivos .json para iniciar a captura.", connectorsDir);
                return;
            }

            ObjectMapper mapper = new ObjectMapper();

            Files.list(dirPath)
                    .filter(path -> path.toString().endsWith(".json"))
                    .forEach(path -> {
                        try {
                            JsonNode rootNode = mapper.readTree(path.toFile());
                            String connectorName = rootNode.get("name").asText();
                            String connectorClass = rootNode.get("class").asText();
                            JsonNode configNode = rootNode.get("config");

                            Properties props = new Properties();
                            configNode.fieldNames().forEachRemaining(fieldName -> {
                                props.setProperty(fieldName, configNode.get(fieldName).asText());
                            });

                            ProcessChangeEventUseCase useCase = new ProcessChangeEventUseCase(publisher, offsetStore, connectorName, meterRegistry);

                            Class<?> clazz = Class.forName(connectorClass);
                            ChangeLogConnector connector = (ChangeLogConnector) clazz.getDeclaredConstructor().newInstance();

                            connector.initialize(connectorName, props, useCase, offsetStore, meterRegistry);
                            engine.registerAndStart(connector);

                            log.info("Conector [{}] carregado com sucesso a partir de {}", connectorName, path.getFileName());

                        } catch (Exception e) {
                            log.error("Falha ao carregar o conector do arquivo {}: {}", path.getFileName(), e.getMessage());
                        }
                    });
        };
    }
}
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

@Configuration
public class CdcConfig {

    private static final Logger log = LoggerFactory.getLogger(CdcConfig.class);

    @Value("${kafka.bootstrap-servers}")
    private String kafkaBootstrapServers;

    @Value("${cdc.offset-dir:./offsets}")
    private String offsetDir;

    @Value("${cdc.connectors-dir:./connectors}")
    private String connectorsDir;

    @Bean(destroyMethod = "close")
    public EventPublisherPort eventPublisherPort() {
        return new KafkaPublisherAdapter(kafkaBootstrapServers);
    }

    @Bean
    public OffsetStorePort offsetStorePort() {
        return new FileOffsetStoreAdapter(offsetDir);
    }

    @Bean(destroyMethod = "shutdown")
    public CdcEngine cdcEngine() {
        return new CdcEngine();
    }

    @Bean
    public ApplicationRunner startCdcRunner(CdcEngine engine,
                                            EventPublisherPort publisher,
                                            OffsetStorePort offsetStore) {
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

                            ProcessChangeEventUseCase useCase = new ProcessChangeEventUseCase(publisher, offsetStore, connectorName);

                            Class<?> clazz = Class.forName(connectorClass);
                            ChangeLogConnector connector = (ChangeLogConnector) clazz.getDeclaredConstructor().newInstance();

                            connector.initialize(connectorName, props, useCase, offsetStore);
                            engine.registerAndStart(connector);

                            log.info("Conector [{}] carregado com sucesso a partir de {}", connectorName, path.getFileName());

                        } catch (Exception e) {
                            log.error("Falha ao carregar o conector do arquivo {}: {}", path.getFileName(), e.getMessage());
                        }
                    });
        };
    }
}
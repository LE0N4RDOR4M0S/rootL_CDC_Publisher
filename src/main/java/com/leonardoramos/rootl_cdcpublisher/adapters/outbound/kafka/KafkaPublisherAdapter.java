package com.leonardoramos.rootl_cdcpublisher.adapters.outbound.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.leonardoramos.rootl_cdcpublisher.application.ports.outbound.EventPublisherPort;
import com.leonardoramos.rootl_cdcpublisher.domain.model.ChangeEvent;
import com.leonardoramos.rootl_cdcpublisher.domain.model.SourceMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Adapter para publicar eventos de mudança (ChangeEvent) no Kafka.
 * Este adaptador é responsável por converter os eventos em mensagens JSON e enviá-los para os tópicos Kafka apropriados.
 */
public class KafkaPublisherAdapter implements EventPublisherPort {

    private static final Logger log = LoggerFactory.getLogger(KafkaPublisherAdapter.class);

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper;

    public KafkaPublisherAdapter(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        this.producer = new KafkaProducer<>(props);
        this.objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }

    /**
     * Publica um evento de mudança no Kafka. O tópico é determinado pelo conector, esquema e tabela de origem do evento.
     * @param event O evento de mudança a ser publicado.
     */
    @Override
    public void publish(ChangeEvent event) {
        SourceMetadata source = event.source();
        if ("system".equals(source.schema())) {
            return;
        }
        try {
            String topicName = String.format("cdc.%s.%s.%s",
                    source.connectorName(),
                    source.schema(),
                    source.table());

            String key = event.eventId().toString();
            String value = objectMapper.writeValueAsString(event);

            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Falha ao publicar evento no Kafka para o tópico {}", topicName, exception);
                } else {
                    log.debug("Evento publicado com sucesso: Tópico={}, Partição={}, Offset={}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                }
            });

        } catch (Exception e) {
            log.error("Erro ao serializar o evento {}", event.eventId(), e);
            throw new RuntimeException("Falha na publicação do evento", e);
        }
    }

    /**
     * Fecha o produtor Kafka, liberando os recursos associados. Deve ser chamado quando o adaptador não for mais necessário.
     */
    public void close() {
        if (this.producer != null) {
            this.producer.close();
        }
    }
}

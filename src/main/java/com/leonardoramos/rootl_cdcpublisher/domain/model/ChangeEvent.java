package com.leonardoramos.rootl_cdcpublisher.domain.model;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Formato canonico para representar eventos de mudança capturados pelo CDC.
 * @param eventId Identificador único do evento, utilizado para rastreamento e idempotência
 * @param operation Tipo de operação (INSERT, UPDATE, DELETE, READ, BEGIN, COMMIT) representando a natureza da mudança
 * @param timestamp Momento exato em que a mudança ocorreu na origem, utilizado para ordenação e análise temporal dos eventos
 * @param source Metadados de origem do evento, incluindo informações sobre o conector, esquema, tabela e transação associada
 * @param before Estado dos dados antes da mudança, representado como um mapa de colunas e valores (pode ser nulo para operações de INSERT)
 * @param after Estado dos dados depois da mudança, representado como um mapa de colunas e valores (pode ser nulo para operações de DELETE)
 */
public record ChangeEvent(
        UUID eventId,
        OperationType operation,
        Instant timestamp,
        SourceMetadata source,
        Map<String, Object> before,
        Map<String, Object> after
) {
    public ChangeEvent {
        if (eventId == null) throw new IllegalArgumentException("eventId não pode ser nulo");
        if (operation == null) throw new IllegalArgumentException("operation não pode ser nula");
        if (source == null) throw new IllegalArgumentException("source metadata não pode ser nulo");
    }
}

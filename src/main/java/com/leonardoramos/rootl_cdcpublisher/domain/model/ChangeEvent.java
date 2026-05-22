package com.leonardoramos.rootl_cdcpublisher.domain.model;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

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

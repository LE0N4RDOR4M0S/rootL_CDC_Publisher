package com.leonardoramos.rootl_cdcpublisher.domain.model;

import java.util.Map;

public record SourceMetadata(
        String connectorName,
        String connectorType,
        String database,
        String schema,
        String table,
        String transactionId,
        Map<String, String> offsetCoordinates
) {
    public static SourceMetadata postgres(String connectorName,String db, String schema, String table, String txId, String lsn) {
        return new SourceMetadata(connectorName,"postgresql", db, schema, table, txId, Map.of("lsn", lsn));
    }
}
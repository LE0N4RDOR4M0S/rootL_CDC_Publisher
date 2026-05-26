package com.leonardoramos.rootl_cdcpublisher.domain.model;

import java.util.Map;

/**
 * Metadados de origem do evento de mudança, contendo informações sobre o conector, esquema, tabela e transação associada.
 * @param connectorName Nome do conector de origem, utilizado para identificar a fonte dos dados (ex: "postgres-connector-1")
 * @param connectorType Tipo do conector de origem, representando a tecnologia ou sistema de onde os dados foram capturados (ex: "postgresql", "mysql", "mongodb")
 * @param database Nome do banco de dados de origem, utilizado para identificar o contexto dos dados (ex: "sales_db")
 * @param schema Nome do esquema de origem, utilizado para organizar e categorizar as tabelas dentro do banco de dados (ex: "public")
 * @param table Nome da tabela de origem, utilizado para identificar a entidade específica que sofreu a mudança (ex: "orders")
 * @param transactionId Identificador da transação associada à mudança, utilizado para rastrear e correlacionar eventos dentro de uma mesma transação (ex: "tx12345")
 * @param offsetCoordinates Mapa de coordenadas de offset, contendo informações específicas do conector para rastrear a posição do evento na origem (ex: {"lsn": "0/16B6C50"})
 */
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
package com.leonardoramos.rootl_cdcpublisher.adapters.inbound.postgres;

import com.leonardoramos.rootl_cdcpublisher.domain.model.ChangeEvent;
import com.leonardoramos.rootl_cdcpublisher.domain.model.OperationType;
import com.leonardoramos.rootl_cdcpublisher.domain.model.SourceMetadata;
import org.postgresql.replication.LogSequenceNumber;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

public class PgOutputDecoder {

    private record Relation(String schema, String table, List<String> columnNames) {}

    private final Map<Integer, Relation> relationCache = new HashMap<>();
    private final String connectorName;
    private final String databaseName;
    private String currentTransactionId = "0";

    public PgOutputDecoder(String connectorName, String databaseName) {
        this.connectorName = connectorName;
        this.databaseName = databaseName;
    }

    public Optional<ChangeEvent> decode(ByteBuffer buffer, LogSequenceNumber lsn) {
        if (!buffer.hasRemaining()) return Optional.empty();

        char messageType = (char) buffer.get();

        switch (messageType) {
            case 'B':
                buffer.getLong();
                long beginTimestamp = buffer.getLong();
                this.currentTransactionId = String.valueOf(buffer.getInt());
                return Optional.of(createControlEvent(OperationType.BEGIN, lsn, parsePgTimestamp(beginTimestamp)));

            case 'R':
                int relationId = buffer.getInt();
                String schema = readNullTerminatedString(buffer);
                String table = readNullTerminatedString(buffer);

                buffer.get();
                short columnCount = buffer.getShort();

                List<String> columns = new ArrayList<>();
                for (int i = 0; i < columnCount; i++) {
                    buffer.get();
                    columns.add(readNullTerminatedString(buffer));
                    buffer.getInt();
                    buffer.getInt();
                }

                relationCache.put(relationId, new Relation(schema, table, columns));
                return Optional.empty();

            case 'I':
                int insertRelationId = buffer.getInt();
                Relation insRel = relationCache.get(insertRelationId);
                if (insRel == null) return Optional.empty();

                buffer.get();
                Map<String, Object> afterColumns = parseTuple(buffer, insRel.columnNames());

                return Optional.of(new ChangeEvent(UUID.randomUUID(), OperationType.INSERT, Instant.now(),
                        SourceMetadata.postgres(connectorName,databaseName, insRel.schema(), insRel.table(), currentTransactionId, lsn.asString()),
                        null, afterColumns));

            case 'U':
                int updateRelationId = buffer.getInt();
                Relation updRel = relationCache.get(updateRelationId);
                if (updRel == null) return Optional.empty();

                char updateKeyIndicator = (char) buffer.get();
                Map<String, Object> beforeColumns = null;

                if (updateKeyIndicator == 'O' || updateKeyIndicator == 'K') {
                    beforeColumns = parseTuple(buffer, updRel.columnNames());
                    buffer.get();
                }
                Map<String, Object> updatedAfterColumns = parseTuple(buffer, updRel.columnNames());

                return Optional.of(new ChangeEvent(UUID.randomUUID(), OperationType.UPDATE, Instant.now(),
                        SourceMetadata.postgres(connectorName,databaseName, updRel.schema(), updRel.table(), currentTransactionId, lsn.asString()),
                        beforeColumns, updatedAfterColumns));

            case 'D':
                int deleteRelationId = buffer.getInt();
                Relation delRel = relationCache.get(deleteRelationId);
                if (delRel == null) return Optional.empty();

                buffer.get();
                Map<String, Object> beforeDeleteColumns = parseTuple(buffer, delRel.columnNames());

                return Optional.of(new ChangeEvent(UUID.randomUUID(), OperationType.DELETE, Instant.now(),
                        SourceMetadata.postgres(connectorName,databaseName, delRel.schema(), delRel.table(), currentTransactionId, lsn.asString()),
                        beforeDeleteColumns, null));

            case 'C':
                buffer.get(); buffer.getLong(); buffer.getLong();
                long commitTimestamp = buffer.getLong();
                return Optional.of(createControlEvent(OperationType.COMMIT, lsn, parsePgTimestamp(commitTimestamp)));

            default:
                return Optional.empty();
        }
    }

    private ChangeEvent createControlEvent(OperationType op, LogSequenceNumber lsn, Instant ts) {
        return new ChangeEvent(UUID.randomUUID(), op, ts,
                SourceMetadata.postgres(connectorName,databaseName, "system", "transaction", currentTransactionId, lsn.asString()), null, null);
    }

    private Map<String, Object> parseTuple(ByteBuffer buffer, List<String> columnNames) {
        short columnCount = buffer.getShort();
        Map<String, Object> columns = new LinkedHashMap<>();

        for (int i = 0; i < columnCount; i++) {
            char type = (char) buffer.get();
            String colName = (i < columnNames.size()) ? columnNames.get(i) : "col_unknown_" + i;

            if (type == 't') {
                int length = buffer.getInt();
                byte[] bytes = new byte[length];
                buffer.get(bytes);
                columns.put(colName, new String(bytes, StandardCharsets.UTF_8));
            } else if (type == 'n') {
                columns.put(colName, null);
            } else if (type == 'u') {
                columns.put(colName, "__TOASTED_VALUE__");
            }
        }
        return columns;
    }

    private String readNullTerminatedString(ByteBuffer buffer) {
        List<Byte> bytes = new ArrayList<>();
        byte b;
        while ((b = buffer.get()) != 0) {
            bytes.add(b);
        }
        byte[] array = new byte[bytes.size()];
        for (int i = 0; i < bytes.size(); i++) array[i] = bytes.get(i);
        return new String(array, StandardCharsets.UTF_8);
    }

    private Instant parsePgTimestamp(long pgMicroseconds) {
        long postgresEpochSeconds = 946684800L;
        long seconds = postgresEpochSeconds + (pgMicroseconds / 1_000_000);
        long nanos = (pgMicroseconds % 1_000_000) * 1000;
        return Instant.ofEpochSecond(seconds, nanos);
    }
}
package com.leonardoramos.rootl_cdcpublisher.adapters.inbound.postgres;

import com.leonardoramos.rootl_cdcpublisher.domain.model.ChangeEvent;
import com.leonardoramos.rootl_cdcpublisher.domain.model.OperationType;
import com.leonardoramos.rootl_cdcpublisher.domain.model.SourceMetadata;
import org.postgresql.replication.LogSequenceNumber;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

/**
 * Classe responsável por decodificar os dados do PostgreSQL Logical Replication Protocol (pgoutput) e transformá-los em eventos de mudança (ChangeEvent) que podem ser processados pelo sistema.
 * O decodificador interpreta os diferentes tipos de mensagens do protocolo, como início de transação, definição de relação, inserção, atualização e exclusão, e extrai as informações relevantes para criar eventos de mudança estruturados.
 * Ele mantém um cache de relações para mapear os IDs de relação aos seus respectivos esquemas, tabelas e nomes de colunas, permitindo a construção correta dos eventos de mudança com base nas mensagens recebidas.
 */
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

    /**
     * Decodifica os dados do buffer de acordo com o protocolo pgoutput, identificando o tipo de mensagem e extraindo as informações relevantes para criar um evento de mudança (ChangeEvent) ou um evento de controle (BEGIN/COMMIT).
     * @param buffer O buffer contendo os dados da mensagem do protocolo pgoutput a ser decodificada.
     * @param lsn O Log Sequence Number (LSN) associado à mensagem, utilizado para rastrear a posição no log de transações do PostgreSQL.
     * @return Um Optional contendo o ChangeEvent decodificado, ou vazio se a mensagem não puder ser decodificada ou se for uma mensagem de definição de relação (que não gera um evento de mudança).
     */
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

    /**
     * Cria um evento de controle (ChangeEvent) para operações de início de transação (BEGIN) ou commit de transação (COMMIT), utilizando as informações do tipo de operação, Log Sequence Number (LSN) e timestamp fornecidos.
     * @param op O tipo de operação (OperationType) que representa o evento de controle a ser criado, como BEGIN ou COMMIT.
     * @param lsn O Log Sequence Number (LSN) associado ao evento de controle, utilizado para rastrear a posição no log de transações do PostgreSQL.
     * @param ts O timestamp do evento de controle, representando o momento em que a operação de controle ocorreu, convertido para o formato Instant.
     * @return Um ChangeEvent representando o evento de controle criado, contendo as informações do tipo de operação, timestamp e metadata de origem associada ao conector, banco de dados, esquema, tabela, ID da transação e LSN.
     */
    private ChangeEvent createControlEvent(OperationType op, LogSequenceNumber lsn, Instant ts) {
        return new ChangeEvent(UUID.randomUUID(), op, ts,
                SourceMetadata.postgres(connectorName,databaseName, "system", "transaction", currentTransactionId, lsn.asString()), null, null);
    }

    /**
     * Analisa um tuple (registro) do buffer de acordo com o protocolo pgoutput, extraindo os valores das colunas com base nos tipos de dados indicados e associando-os aos nomes das colunas fornecidos. O método suporta tipos de dados de texto, nulos e valores "toasted" (valores grandes armazenados fora da linha).
     * @param buffer O buffer contendo os dados do tuple a ser analisado, posicionado no início do registro a ser lido.
     * @param columnNames A lista de nomes das colunas associadas ao tuple, utilizada para mapear os valores extraídos aos nomes corretos das colunas. Se o número de colunas no tuple exceder o número de nomes fornecidos, os nomes das colunas adicionais serão gerados como "col_unknown_X", onde X é o índice da coluna.
     * @return Um mapa contendo os nomes das colunas como chaves e os valores correspondentes como valores, representando os dados do tuple extraídos do buffer. Os valores podem ser strings, nulos ou um marcador para valores "toasted", dependendo do tipo de dado indicado no buffer.
     */
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

    /**
     * Lê uma string terminada em nulo (null-terminated) do buffer, construindo a string a partir dos bytes lidos até encontrar o byte nulo (0). O método utiliza a codificação UTF-8 para converter os bytes lidos em uma string legível.
     * @param buffer O buffer do qual a string terminada em nulo deve ser lida, posicionado no início da string a ser lida. O método continuará lendo bytes até encontrar um byte nulo (0), indicando o final da string.
     * @return A string lida do buffer, construída a partir dos bytes lidos até o byte nulo, utilizando a codificação UTF-8. Se o buffer não contiver um byte nulo, o método pode lançar uma exceção ou retornar uma string incompleta, dependendo do comportamento do ByteBuffer ao tentar ler além de seus limites.
     */
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

    /**
     * Converte um timestamp do PostgreSQL, representado como o número de microssegundos desde a época do PostgreSQL (1º de janeiro de 2000), para um objeto Instant do Java, que representa um ponto no tempo com precisão de nanossegundos. O método calcula os segundos e nanossegundos correspondentes ao timestamp do PostgreSQL e os utiliza para criar o Instant resultante.
     * @param pgMicroseconds O timestamp do PostgreSQL a ser convertido, representado como o número de microssegundos desde a época do PostgreSQL (1º de janeiro de 2000). O valor deve ser um número inteiro que representa a quantidade total de microssegundos desde essa data.
     * @return Um objeto Instant do Java que representa o mesmo ponto no tempo que o timestamp do PostgreSQL fornecido, convertido para a precisão de nanossegundos. O Instant resultante pode ser utilizado para representar datas e horas em operações de data/hora no Java, como comparação, formatação e manipulação de tempo.
     */
    private Instant parsePgTimestamp(long pgMicroseconds) {
        long postgresEpochSeconds = 946684800L;
        long seconds = postgresEpochSeconds + (pgMicroseconds / 1_000_000);
        long nanos = (pgMicroseconds % 1_000_000) * 1000;
        return Instant.ofEpochSecond(seconds, nanos);
    }
}
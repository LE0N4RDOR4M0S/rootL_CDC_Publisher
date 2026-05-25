package com.leonardoramos.rootl_cdcpublisher.adapters.inbound.mysql;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.leonardoramos.rootl_cdcpublisher.application.ports.inbound.ChangeLogConnector;
import com.leonardoramos.rootl_cdcpublisher.application.ports.outbound.OffsetStorePort;
import com.leonardoramos.rootl_cdcpublisher.application.usecases.ProcessChangeEventUseCase;
import com.leonardoramos.rootl_cdcpublisher.domain.model.ChangeEvent;
import com.leonardoramos.rootl_cdcpublisher.domain.model.OperationType;
import com.leonardoramos.rootl_cdcpublisher.domain.model.SourceMetadata;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class MySqlBinlogAdapter implements ChangeLogConnector {

    private static final Logger log = LoggerFactory.getLogger(MySqlBinlogAdapter.class);

    private String connectorId;
    private String host;
    private int port;
    private String user;
    private String password;
    private String database;

    private ProcessChangeEventUseCase useCase;
    private OffsetStorePort offsetStore;
    private BinaryLogClient client;
    private MySqlSchemaCache schemaCache;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Map<Long, String> tableMap = new HashMap<>();

    public MySqlBinlogAdapter() {}

    @Override
    public void initialize(String connectorId, Properties config, ProcessChangeEventUseCase useCase, OffsetStorePort offsetStore, MeterRegistry registry) {
        this.connectorId = connectorId;
        this.host = config.getProperty("host");
        this.port = Integer.parseInt(config.getProperty("port", "3306"));
        this.user = config.getProperty("user");
        this.password = config.getProperty("password");
        this.database = config.getProperty("database");

        this.useCase = useCase;
        this.offsetStore = offsetStore;

        String jdbcUrl = String.format("jdbc:mysql://%s:%d/%s", host, port, database);
        this.schemaCache = new MySqlSchemaCache(jdbcUrl, user, password);

        Gauge.builder("cdc.connector.status", this.running, r -> r.get() ? 1.0 : 0.0)
                .tag("connector", connectorId)
                .tag("type", getType())
                .description("Status de execução da thread do conector")
                .register(registry);

        log.info("Conector MySQL '{}' inicializado e métricas registradas.", connectorId);
    }

    @Override
    public String getType() {
        return "mysql";
    }

    @Override
    public void start() {
        if (running.getAndSet(true)) return;

        new Thread(() -> {
            try {
                executeSnapshotSeNecessario();

                client = new BinaryLogClient(host, port, user, password);

                var lastOffset = offsetStore.load(connectorId);
                if (lastOffset.isPresent()) {
                    String[] parts = lastOffset.get().split(":");
                    client.setBinlogFilename(parts[0]);
                    client.setBinlogPosition(Long.parseLong(parts[1]));
                    log.info("Iniciando leitura do MySQL Binlog a partir de {}:{}", parts[0], parts[1]);
                } else {
                    log.warn("Nenhum offset base encontrado após a fase de inicialização.");
                }

                client.registerEventListener(this::handleEvent);
                client.connect();

            } catch (Exception e) {
                log.error("Falha crítica no stream do MySQL", e);
                running.set(false);
            }
        }, "worker-" + connectorId).start();
    }

    @Override
    public void stop() {
        if (!running.getAndSet(false)) return;
        try {
            if (client != null) client.disconnect();
        } catch (Exception e) {
            log.warn("Erro ao desconectar MySQL Binlog", e);
        }
    }

    private void executeSnapshotSeNecessario() throws Exception {
        if (offsetStore.load(connectorId).isPresent()) {
            return;
        }

        log.info("Nenhum offset encontrado. Iniciando Carga Inicial (Snapshot Seguro) no MySQL...");
        String jdbcUrl = String.format("jdbc:mysql://%s:%d/%s", host, port, database);

        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password)) {
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

            String binlogFile = "";
            long binlogPos = 0;

            try (Statement stmt = conn.createStatement()) {
                log.info("Aplicando bloqueio global de leitura (Read Lock)...");
                stmt.execute("FLUSH TABLES WITH READ LOCK");

                try {
                    try (ResultSet rs = stmt.executeQuery("SHOW MASTER STATUS")) {
                        if (rs.next()) {
                            binlogFile = rs.getString("File");
                            binlogPos = rs.getLong("Position");
                        } else {
                            throw new RuntimeException("Log binário não está ativo no MySQL! Verifique o my.cnf.");
                        }
                    }
                    stmt.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT");
                } finally {
                    stmt.execute("UNLOCK TABLES");
                    log.info("Bloqueio global liberado. Produção destravada.");
                }

                String snapshotOffset = binlogFile + ":" + binlogPos;
                String fakeTxId = "snapshot-tx";

                useCase.process(new ChangeEvent(UUID.randomUUID(), OperationType.BEGIN, Instant.now(),
                        createMetadata(database, "system", "transaction", fakeTxId, snapshotOffset), null, null));

                List<String> tablesToSnapshot = new ArrayList<>();
                try (ResultSet rsTables = stmt.executeQuery("SHOW TABLES")) {
                    while (rsTables.next()) {
                        tablesToSnapshot.add(rsTables.getString(1));
                    }
                }

                int totalRegistros = 0;
                for (String table : tablesToSnapshot) {
                    log.info("Efetuando snapshot da tabela MySQL: {}", table);
                    List<String> columnNames = schemaCache.getColumns(database, table);

                    try (ResultSet rsData = stmt.executeQuery("SELECT * FROM " + table)) {
                        while (rsData.next()) {
                            Map<String, Object> afterColumns = extrairColunasDoResultSet(rsData, columnNames);

                            ChangeEvent event = new ChangeEvent(UUID.randomUUID(), OperationType.READ, Instant.now(),
                                    createMetadata(database, database, table, fakeTxId, snapshotOffset), null, afterColumns);

                            useCase.process(event);
                            totalRegistros++;
                        }
                    }
                }

                useCase.process(new ChangeEvent(UUID.randomUUID(), OperationType.COMMIT, Instant.now(),
                        createMetadata(database, "system", "transaction", fakeTxId, snapshotOffset), null, null));

                conn.commit();
                log.info("Snapshot finalizado! {} registros publicados. Posição base do Binlog: {}", totalRegistros, snapshotOffset);

            } catch (Exception e) {
                conn.rollback();
                throw new RuntimeException("Falha ao executar o snapshot do MySQL", e);
            }
        }
    }

    private Map<String, Object> extrairColunasDoResultSet(ResultSet rs, List<String> columnNames) throws SQLException {
        Map<String, Object> columns = new LinkedHashMap<>();
        for (int i = 0; i < columnNames.size(); i++) {
            columns.put(columnNames.get(i), rs.getString(i + 1));
        }
        return columns;
    }

    private void handleEvent(Event event) {
        EventHeaderV4 header = event.getHeader();
        EventData data = event.getData();

        String binlogFile = client.getBinlogFilename();
        String binlogPos = String.valueOf(header.getPosition());
        String offsetCoordinates = binlogFile + ":" + binlogPos;
        String txId = "tx-" + header.getServerId() + "-" + header.getPosition();

        if (data instanceof TableMapEventData tableData) {
            tableMap.put(tableData.getTableId(), tableData.getDatabase() + "." + tableData.getTable());

        } else if (data instanceof WriteRowsEventData writeData) {
            processRowEvent(writeData.getTableId(), OperationType.INSERT, null, writeData.getRows(), txId, offsetCoordinates);

        } else if (data instanceof UpdateRowsEventData updateData) {
            for (Map.Entry<Serializable[], Serializable[]> row : updateData.getRows()) {
                processRowEvent(updateData.getTableId(), OperationType.UPDATE, row.getKey(),
                        Collections.singletonList(row.getValue()), txId, offsetCoordinates);
            }

        } else if (data instanceof DeleteRowsEventData deleteData) {
            processRowEvent(deleteData.getTableId(), OperationType.DELETE, null, deleteData.getRows(), txId, offsetCoordinates);

        } else if (data instanceof XidEventData) {
            ChangeEvent commitEvent = new ChangeEvent(UUID.randomUUID(), OperationType.COMMIT, Instant.now(),
                    createMetadata(database, "system", "transaction", txId, offsetCoordinates), null, null);
            useCase.process(commitEvent);
        }
    }

    private void processRowEvent(long tableId, OperationType op, Serializable[] beforeArray, List<Serializable[]> rows, String txId, String offset) {
        String dbAndTable = tableMap.get(tableId);
        if (dbAndTable == null) return;

        String[] parts = dbAndTable.split("\\.");
        String db = parts[0];
        String table = parts[1];

        if (!db.equals(database)) return;

        List<String> columnNames = schemaCache.getColumns(db, table);
        SourceMetadata metadata = createMetadata(db, db, table, txId, offset);

        for (Serializable[] rowArray : rows) {
            Map<String, Object> beforeData = (beforeArray != null) ? mapColumns(columnNames, beforeArray) : null;
            Map<String, Object> afterData = (op != OperationType.DELETE) ? mapColumns(columnNames, rowArray) : null;

            if (op == OperationType.DELETE) {
                beforeData = mapColumns(columnNames, rowArray);
            }

            ChangeEvent event = new ChangeEvent(UUID.randomUUID(), op, Instant.now(), metadata, beforeData, afterData);
            useCase.process(event);
        }
    }

    private Map<String, Object> mapColumns(List<String> columnNames, Serializable[] row) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < row.length; i++) {
            String colName = (i < columnNames.size()) ? columnNames.get(i) : "col_" + i;
            map.put(colName, (row[i] != null) ? row[i].toString() : null);
        }
        return map;
    }

    private SourceMetadata createMetadata(String db, String schema, String table, String txId, String offsetString) {
        return new SourceMetadata(connectorId, "mysql", db, schema, table, txId, Map.of("binlog_pos", offsetString));
    }
}
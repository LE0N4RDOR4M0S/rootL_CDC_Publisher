package com.leonardoramos.rootl_cdcpublisher.adapters.inbound.oracle;

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

import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class OracleLogMinerAdapter implements ChangeLogConnector {

    private static final Logger log = LoggerFactory.getLogger(OracleLogMinerAdapter.class);

    private String connectorId;
    private String jdbcUrl;
    private String user;
    private String password;
    private String schemaTarget;
    private String containerTarget;

    private ProcessChangeEventUseCase useCase;
    private OffsetStorePort offsetStore;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private long currentScn = 0;

    public OracleLogMinerAdapter() {}

    @Override
    public void initialize(String connectorId, Properties config, ProcessChangeEventUseCase useCase, OffsetStorePort offsetStore, MeterRegistry registry) {
        this.connectorId = connectorId;
        this.jdbcUrl = config.getProperty("jdbcUrl");
        this.user = config.getProperty("user");
        this.password = config.getProperty("password");
        this.schemaTarget = config.getProperty("schema").toUpperCase();

        String container = config.getProperty("container");
        this.containerTarget = (container != null) ? container.toUpperCase() : null;

        this.useCase = useCase;
        this.offsetStore = offsetStore;

        Gauge.builder("cdc.connector.status", this.running, r -> r.get() ? 1.0 : 0.0)
                .tag("connector", connectorId)
                .tag("type", getType())
                .description("Status de execução do conector Oracle")
                .register(registry);
    }

    @Override
    public String getType() { return "oracle"; }

    @Override
    public void start() {
        if (running.getAndSet(true)) return;

        new Thread(this::mineLogLoop, "worker-" + connectorId).start();
    }

    private void mineLogLoop() {
        try {
            var lastOffset = offsetStore.load(connectorId);
            if (lastOffset.isPresent()) {
                this.currentScn = Long.parseLong(lastOffset.get());
                log.info("Retomando Oracle LogMiner a partir do SCN: {}", currentScn);
            } else {
                this.currentScn = fetchCurrentScnFromServer();
                log.info("Nenhum offset encontrado. Iniciando Oracle LogMiner do SCN estável: {}", currentScn);
            }

            while (running.get()) {
                try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password)) {

                    if (this.containerTarget != null && !this.containerTarget.isBlank()) {
                        try (Statement stmt = conn.createStatement()) {
                            stmt.execute("ALTER SESSION SET CONTAINER = " + this.containerTarget);
                            log.trace("[{}] Sessão direcionada para o container: {}", connectorId, containerTarget);
                        } catch (Exception e) {
                            log.error("[{}] Falha ao alternar para o container {}: {}", connectorId, containerTarget, e.getMessage());
                            throw e;
                        }
                    }

                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute("BEGIN DBMS_LOGMNR.START_LOGMNR(" +
                                "STARTSCN => " + currentScn + ", " +
                                "OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.COMMITTED_DATA_ONLY); END;");
                    }

                    long scnDePartidaDestaIteracao = this.currentScn;

                    String query = "SELECT SCN, SQL_REDO, OPERATION, TABLE_NAME, SEG_OWNER, TX_NAME, TIMESTAMP " +
                            "FROM V$LOGMNR_CONTENTS WHERE SEG_OWNER = ? AND SCN >= ? ORDER BY SCN ASC";

                    try (PreparedStatement ps = conn.prepareStatement(query)) {
                        ps.setString(1, schemaTarget);
                        ps.setLong(2, currentScn);

                        try (ResultSet rs = ps.executeQuery()) {
                            while (rs.next() && running.get()) {
                                long scn = rs.getLong("SCN");
                                String sqlRedo = rs.getString("SQL_REDO");
                                String operation = rs.getString("OPERATION");
                                String tableName = rs.getString("TABLE_NAME");
                                String txId = rs.getString("TX_NAME") != null ? rs.getString("TX_NAME") : "ora-tx-" + scn;
                                Timestamp timestamp = rs.getTimestamp("TIMESTAMP");

                                if (sqlRedo == null || sqlRedo.isEmpty()) continue;

                                if (scn == scnDePartidaDestaIteracao) {
                                    continue;
                                }

                                log.info("Mutação detectada no Oracle! Operação: {}, Tabela: {}, SCN: {}", operation, tableName, scn);
                                processLogMinerRecord(operation, sqlRedo, tableName, txId, scn, timestamp);

                                RepublicarOuAtualizarPonteiro(scn);
                            }
                        }
                    }

                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute("BEGIN DBMS_LOGMNR.END_LOGMNR; END;");
                    }

                } catch (Exception e) {
                    log.warn("Erro no ciclo do LogMiner: {}. Tentando novamente em 5 segundos...", e.getMessage(), e);
                    Thread.sleep(5000L);
                    continue;
                }

                Thread.sleep(1000L);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void RepublicarOuAtualizarPonteiro(long scn) {
        this.currentScn = scn;
        offsetStore.save(connectorId, String.valueOf(scn));
    }

    private void processLogMinerRecord(String operation, String sql, String table, String txId, long scn, Timestamp ts) {
        OperationType op = translateOperation(operation);
        if (op == null) return;

        SourceMetadata metadata = new SourceMetadata(connectorId, "oracle", schemaTarget, schemaTarget, table, txId, Map.of("scn", String.valueOf(scn)));
        Instant eventTime = ts != null ? ts.toInstant() : Instant.now();

        useCase.process(new ChangeEvent(UUID.randomUUID(), OperationType.BEGIN, eventTime, metadata, null, null));

        Map<String, Object> before = null;
        Map<String, Object> after = null;

        if (op == OperationType.INSERT) {
            after = OracleSqlParser.parseInsert(sql);
        } else if (op == OperationType.UPDATE) {
            Map<String, Object>[] structures = OracleSqlParser.parseUpdateWithDelta(sql);
            before = structures[0];
            after = structures[1];
        } else if (op == OperationType.DELETE) {
            before = OracleSqlParser.parseInsert(sql);
        }

        ChangeEvent dataEvent = new ChangeEvent(UUID.randomUUID(), op, eventTime, metadata, before, after);
        useCase.process(dataEvent);

        useCase.process(new ChangeEvent(UUID.randomUUID(), OperationType.COMMIT, eventTime, metadata, null, null));
    }

    private OperationType translateOperation(String op) {
        return switch (op) {
            case "INSERT" -> OperationType.INSERT;
            case "UPDATE" -> OperationType.UPDATE;
            case "DELETE" -> OperationType.DELETE;
            default -> null;
        };
    }

    private long fetchCurrentScnFromServer() {
        String query = "SELECT MIN(FIRST_CHANGE#) FROM V$LOG WHERE STATUS = 'CURRENT' OR STATUS = 'ACTIVE'";

        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) {
                long scn = rs.getLong(1);
                if (scn > 0) {
                    log.info("SCN inicial seguro extraído do Redo Log ativo: {}", scn);
                    return scn;
                }
            }
        } catch (Exception e) {
            log.error("Não foi possível buscar o SCN estável do V$LOG", e);
        }

        return 0;
    }

    @Override
    public void stop() {
        running.set(false);
    }
}
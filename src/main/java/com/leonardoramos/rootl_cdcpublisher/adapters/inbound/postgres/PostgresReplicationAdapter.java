package com.leonardoramos.rootl_cdcpublisher.adapters.inbound.postgres;

import com.leonardoramos.rootl_cdcpublisher.application.ports.inbound.ChangeLogConnector;
import com.leonardoramos.rootl_cdcpublisher.application.ports.outbound.OffsetStorePort;
import com.leonardoramos.rootl_cdcpublisher.application.usecases.ProcessChangeEventUseCase;
import com.leonardoramos.rootl_cdcpublisher.domain.model.ChangeEvent;
import com.leonardoramos.rootl_cdcpublisher.domain.model.OperationType;
import com.leonardoramos.rootl_cdcpublisher.domain.model.SourceMetadata;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class PostgresReplicationAdapter implements ChangeLogConnector {

    private static final Logger log = LoggerFactory.getLogger(PostgresReplicationAdapter.class);

    private String jdbcUrl;
    private String user;
    private String password;
    private String slotName;
    private String databaseName;
    private String connectorId;

    private ProcessChangeEventUseCase useCase;
    private OffsetStorePort offsetStore;
    private PgOutputDecoder decoder;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread workerThread;
    private PGReplicationStream stream;

    public PostgresReplicationAdapter() {}

    @Override
    public void initialize(String connectorId, Properties config, ProcessChangeEventUseCase useCase, OffsetStorePort offsetStore) {
        this.connectorId = connectorId;
        this.jdbcUrl = config.getProperty("jdbcUrl");
        this.user = config.getProperty("user");
        this.password = config.getProperty("password");
        this.slotName = config.getProperty("slotName");
        this.databaseName = config.getProperty("database");
        this.useCase = useCase;
        this.offsetStore = offsetStore;
        this.decoder = new PgOutputDecoder(this.connectorId, this.databaseName);

        log.info("Conector '{}' inicializado com as configurações fornecidas.", connectorId);
    }

    @Override
    public String getType() {
        return "postgresql";
    }

    @Override
    public void start() {
        if (running.getAndSet(true)) return;
        workerThread = new Thread(this::replicationLoop, "worker-" + connectorId);
        workerThread.start();
        log.info("Adapter [{}] iniciado com sucesso.", getType());
    }

    @Override
    public void stop() {
        if (!running.getAndSet(false)) return;
        log.info("Interrompendo a captura de logs do conector {}...", connectorId);
        try {
            if (stream != null && !stream.isClosed()) {
                stream.close();
            }
            if (workerThread != null) {
                workerThread.join(5000);
            }
        } catch (Exception e) {
            log.error("Erro ao encerrar os recursos de forma limpa", e);
        }
    }

    private void replicationLoop() {
        while (running.get()) {
            try {
                executeSnapshotSeNecessario();

                PGConnection pgConnection = ReplicationConnectionFactory.create(jdbcUrl, user, password);

                var lastOffset = offsetStore.load(connectorId);
                var logicalStreamBuilder = pgConnection.getReplicationAPI()
                        .replicationStream()
                        .logical()
                        .withSlotName(slotName)
                        .withSlotOption("proto_version", "1")
                        .withSlotOption("publication_names", "cdc_publication");

                if (lastOffset.isPresent()) {
                    String lsnStr = lastOffset.get();
                    log.info("Retomando leitura a partir do LSN guardado: {}", lsnStr);
                    logicalStreamBuilder.withStartPosition(LogSequenceNumber.valueOf(lsnStr));
                }

                this.stream = logicalStreamBuilder.start();
                log.info("Conexão de Replicação estável estabelecida com o slot '{}'", slotName);

                while (running.get()) {
                    ByteBuffer msg = stream.readPending();

                    if (msg == null) {
                        Thread.sleep(10L);
                        continue;
                    }

                    LogSequenceNumber lastReceiveLSN = stream.getLastReceiveLSN();
                    decoder.decode(msg, lastReceiveLSN).ifPresent(useCase::process);

                    stream.setAppliedLSN(lastReceiveLSN);
                    stream.setFlushedLSN(lastReceiveLSN);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Thread de replicação interrompida internamente.");
            } catch (Exception e) {
                log.error("Falha crítica detectada no stream. Tentando reconectar em 5 segundos...", e);
                try {
                    Thread.sleep(5000L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void executeSnapshotSeNecessario() throws SQLException {
        if (offsetStore.load(connectorId).isPresent()) {
            return;
        }

        log.info("Nenhum offset encontrado. Iniciando Carga Inicial (Snapshot Seguro)...");

        String cleanUrl = jdbcUrl.replace("?replication=database", "");

        try (Connection jdbcConnection = DriverManager.getConnection(cleanUrl, user, password)) {

            jdbcConnection.setAutoCommit(false);
            jdbcConnection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            jdbcConnection.setReadOnly(true);

            try (Statement stmt = jdbcConnection.createStatement()) {
                ResultSet rsLsn = stmt.executeQuery("SELECT pg_current_wal_lsn()");
                rsLsn.next();
                String snapshotLsn = rsLsn.getString(1);
                String fakeTxId = "snapshot-tx";

                useCase.process(new ChangeEvent(UUID.randomUUID(), OperationType.BEGIN, Instant.now(),
                        SourceMetadata.postgres(connectorId,databaseName, "system", "transaction", fakeTxId, snapshotLsn), null, null));

                ResultSet rsTables = stmt.executeQuery("SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = 'cdc_publication'");
                List<String> tabelasParaLer = new ArrayList<>();
                while (rsTables.next()) {
                    tabelasParaLer.add(rsTables.getString("schemaname") + "." + rsTables.getString("tablename"));
                }

                int totalRegistros = 0;
                for (String tabela : tabelasParaLer) {
                    log.info("Efetuando snapshot da tabela: {}", tabela);
                    ResultSet rsData = stmt.executeQuery("SELECT * FROM " + tabela);

                    String[] partes = tabela.split("\\.");
                    String schema = partes[0];
                    String tableName = partes[1];

                    while (rsData.next()) {
                        Map<String, Object> afterColumns = extrairColunas(rsData);

                        ChangeEvent event = new ChangeEvent(UUID.randomUUID(), OperationType.READ, Instant.now(),
                                SourceMetadata.postgres(connectorId,databaseName, schema, tableName, fakeTxId, snapshotLsn), null, afterColumns);

                        useCase.process(event);
                        totalRegistros++;
                    }
                }

                useCase.process(new ChangeEvent(UUID.randomUUID(), OperationType.COMMIT, Instant.now(),
                        SourceMetadata.postgres(connectorId,databaseName, "system", "transaction", fakeTxId, snapshotLsn), null, null));

                jdbcConnection.commit();
                log.info("Snapshot finalizado! {} registros publicados. LSN base: {}", totalRegistros, snapshotLsn);

            } catch (Exception e) {
                jdbcConnection.rollback();
                throw new RuntimeException("Falha ao executar o snapshot", e);
            }
        }
    }

    private Map<String, Object> extrairColunas(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        Map<String, Object> columns = new LinkedHashMap<>();

        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnName(i);
            String value = rs.getString(i); // Mantém a tipagem como string igual ao PgOutputDecoder
            columns.put(columnName, value);
        }
        return columns;
    }
}
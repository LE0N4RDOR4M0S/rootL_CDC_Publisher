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

/**
 * Implementação do conector CDC para Oracle usando LogMiner.
 */
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

    // Cache de deduplicação
    private final Set<String> processedEventsAtCurrentScn = new HashSet<>();

    public OracleLogMinerAdapter() {}

    /**
     * Inicializa o conector com as configurações necessárias e casos de uso para processamento.
     * @param connectorId Id do conector, utilizado para logs e métricas
     * @param config Configurações específicas do conector (credenciais, tópicos, etc)
     * @param useCase Casos de uso do domínio para processar os eventos
     * @param offsetStore Armazenamento de offset para garantir processamento idempotente e reinício seguro
     * @param registry Registro de métricas para monitorar o conector
     */
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

    /**
     * Retorna o tipo do conector, utilizado para identificação e roteamento de eventos.
     * @return "oracle"
     */
    @Override
    public String getType() { return "oracle"; }

    /**
     * Inicia o processo de mineração de logs do Oracle
     */
    @Override
    public void start() {
        if (running.getAndSet(true)) return;
        new Thread(this::mineLogLoop, "worker-" + connectorId).start();
    }

    /**
     * Loop principal de mineração de logs do Oracle. Ele gerencia a sessão do LogMiner, descoberta de arquivos de log, leitura e processamento dos eventos, e controle de offset para garantir que os eventos sejam processados na ordem correta e sem duplicações.
     * O loop é projetado para ser resiliente a falhas temporárias, como perda de conexão ou falta de arquivos de log, e inclui uma lógica de retry com backoff para lidar com essas situações.
     * O método também inclui uma lógica de deduplicação baseada em um cache de eventos processados para evitar o reprocessamento de eventos que possam aparecer mais de uma vez devido à forma como o LogMiner apresenta os dados.
     * A implementação assume que o conector tem acesso a um banco Oracle configurado corretamente para permitir o uso do LogMiner, e que as permissões necessárias foram concedidas ao usuário utilizado para a conexão.
     */
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

                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute("ALTER SESSION SET CONTAINER = CDB$ROOT");
                    }

                    boolean logFilesAdded = false;

                    //GROUP BY adicionado para evitar colisão de logs multiplexados
                    String findLogsQuery =
                            "SELECT MIN(NAME) FROM SYS.V_$ARCHIVED_LOG WHERE NEXT_CHANGE# >= " + currentScn + " AND STATUS = 'A' GROUP BY THREAD#, SEQUENCE# " +
                                    "UNION " +
                                    "SELECT MIN(F.MEMBER) AS NAME FROM SYS.V_$LOG L JOIN SYS.V_$LOGFILE F ON L.GROUP# = F.GROUP# WHERE L.STATUS IN ('CURRENT', 'ACTIVE') GROUP BY L.GROUP#";

                    Set<String> uniqueLogs = new LinkedHashSet<>();

                    try (Statement stmt = conn.createStatement();
                         ResultSet rs = stmt.executeQuery(findLogsQuery)) {
                        while (rs.next()) {
                            String logFile = rs.getString(1);
                            if (logFile != null && !logFile.trim().isEmpty()) {
                                uniqueLogs.add(logFile.trim());
                            }
                        }
                    }

                    boolean isFirst = true;
                    for (String logFileName : uniqueLogs) {
                        String option = isFirst ? "DBMS_LOGMNR.NEW" : "DBMS_LOGMNR.ADDFILE";

                        try (Statement addStmt = conn.createStatement()) {
                            addStmt.execute("BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => '" + logFileName + "', OPTIONS => " + option + "); END;");
                            logFilesAdded = true;
                            isFirst = false;
                        } catch (SQLException e) {
                            if (e.getMessage().contains("ORA-01289")) {
                                log.trace("Log {} já está na lista do LogMiner, ignorando duplicidade.", logFileName);
                                logFilesAdded = true;
                                isFirst = false;
                            } else {
                                throw e;
                            }
                        }
                    }

                    if (!logFilesAdded) {
                        log.warn("Nenhum arquivo de log encontrado para cobrir o SCN {}. Aguardando rotação do Oracle...", currentScn);
                        Thread.sleep(5000L);
                        continue;
                    }

                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute("BEGIN DBMS_LOGMNR.START_LOGMNR(" +
                                "STARTSCN => " + currentScn + ", " +
                                "OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG " +
                                "         + DBMS_LOGMNR.COMMITTED_DATA_ONLY); END;");
                    }

                    String query;
                    boolean filtrarPorContainer = (this.containerTarget != null && !this.containerTarget.isBlank());

                    if (filtrarPorContainer) {
                        query = "SELECT SCN, SQL_REDO, OPERATION, TABLE_NAME, SEG_OWNER, TX_NAME, TIMESTAMP " +
                                "FROM SYS.V_$LOGMNR_CONTENTS WHERE SEG_OWNER = ? AND SCN >= ? AND SRC_CON_NAME = ? ORDER BY SCN ASC";
                    } else {
                        query = "SELECT SCN, SQL_REDO, OPERATION, TABLE_NAME, SEG_OWNER, TX_NAME, TIMESTAMP " +
                                "FROM SYS.V_$LOGMNR_CONTENTS WHERE SEG_OWNER = ? AND SCN >= ? ORDER BY SCN ASC";
                    }

                    try (PreparedStatement ps = conn.prepareStatement(query)) {
                        ps.setString(1, schemaTarget);
                        ps.setLong(2, currentScn);
                        if (filtrarPorContainer) {
                            ps.setString(3, containerTarget);
                        }

                        try (ResultSet rs = ps.executeQuery()) {
                            while (rs.next() && running.get()) {
                                long scn = rs.getLong("SCN");
                                String sqlRedo = rs.getString("SQL_REDO");
                                String operation = rs.getString("OPERATION");
                                String tableName = rs.getString("TABLE_NAME");
                                String txId = rs.getString("TX_NAME") != null ? rs.getString("TX_NAME") : "ora-tx-" + scn;
                                Timestamp timestamp = rs.getTimestamp("TIMESTAMP");

                                if (sqlRedo == null || sqlRedo.isEmpty()) continue;

                                if (scn < currentScn) continue;

                                if (scn > currentScn) {
                                    processedEventsAtCurrentScn.clear();
                                    this.currentScn = scn;
                                }

                                String fingerprint = operation + ":" + tableName + ":" + sqlRedo;
                                if (processedEventsAtCurrentScn.contains(fingerprint)) {
                                    continue;
                                }

                                processLogMinerRecord(operation, sqlRedo, tableName, txId, scn, timestamp);

                                processedEventsAtCurrentScn.add(fingerprint);
                                offsetStore.save(connectorId, String.valueOf(scn));
                            }
                        }
                    }

                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute("BEGIN DBMS_LOGMNR.END_LOGMNR; END;");
                    }

                } catch (Exception e) {
                    log.warn("Aviso no ciclo do LogMiner: {}. Tentando novamente em 5 segundos...", e.getMessage());
                    Thread.sleep(5000L);
                    continue;
                }

                Thread.sleep(1000L);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Processa um registro retornado pelo LogMiner, traduzindo a operação e o SQL para um evento de mudança do domínio, e enviando-o para processamento pelos casos de uso.
     * @param operation Tipo de operação (INSERT, UPDATE, DELETE)
     * @param sql SQL de redo fornecido pelo LogMiner, que contém os dados necessários para extrair os valores antes e depois da mudança
     * @param table Nome da tabela afetada pela mudança
     * @param txId Identificador da transação no banco de origem
     * @param scn SCN associado ao evento, utilizado para controle de offset e ordenação dos eventos
     * @param ts Timestamp do evento, utilizado para definir o tempo do evento no domínio
     */
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
            before = OracleSqlParser.parseDelete(sql);
        }

        ChangeEvent dataEvent = new ChangeEvent(UUID.randomUUID(), op, eventTime, metadata, before, after);
        useCase.process(dataEvent);

        useCase.process(new ChangeEvent(UUID.randomUUID(), OperationType.COMMIT, eventTime, metadata, null, null));
    }

    // Traduz o tipo de operação do LogMiner para o modelo de domínio
    private OperationType translateOperation(String op) {
        return switch (op) {
            case "INSERT" -> OperationType.INSERT;
            case "UPDATE" -> OperationType.UPDATE;
            case "DELETE" -> OperationType.DELETE;
            default -> null;
        };
    }

    /**
     * Busca o SCN atual do banco Oracle para iniciar a mineração de logs a partir de um ponto seguro. Ele consulta a view V$LOG para encontrar o menor SCN dos arquivos de log que estão atualmente ativos ou em uso, garantindo que o conector comece a minerar a partir de um ponto que contenha dados consistentes e completos.
     * @return O SCN inicial seguro para iniciar a mineração de logs, ou 0 se não for possível determinar um SCN válido
     */
    private long fetchCurrentScnFromServer() {
        String query = "SELECT MIN(FIRST_CHANGE#) FROM SYS.V_$LOG WHERE STATUS = 'CURRENT' OR STATUS = 'ACTIVE'";

        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("ALTER SESSION SET CONTAINER = CDB$ROOT");
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {
                if (rs.next()) {
                    long scn = rs.getLong(1);
                    if (scn > 0) {
                        log.info("SCN inicial seguro extraído do Redo Log ativo (CDB Level): {}", scn);
                        return scn;
                    }
                }
            }
        } catch (Exception e) {
            log.error("Não foi possível buscar o SCN estável do V$LOG no CDB", e);
        }

        return 0;
    }

    // Encerra o processo de mineração de logs, sinalizando para o loop principal parar e liberando quaisquer recursos necessários
    @Override
    public void stop() {
        running.set(false);
    }
}
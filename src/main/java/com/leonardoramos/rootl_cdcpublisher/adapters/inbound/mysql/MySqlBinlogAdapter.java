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

/**
 * Conector de CDC para MySql a partir de leitura do Binlog.
 */
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

    /**
     * Inicializa o conector MySQL Binlog com as configurações necessárias e registra métricas.
     * @param connectorId Id do conector, utilizado para logs e métricas
     * @param config Configurações específicas do conector (credenciais, tópicos, etc)
     * @param useCase Casos de uso do domínio para processar os eventos
     * @param offsetStore Armazenamento de offset para garantir processamento idempotente e reinício seguro
     * @param registry Registro de métricas para monitorar o conector
     */
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

    /**
     * Retorna o tipo do conector, utilizado para identificação e métricas.
     * @return "mysql"
     */
    @Override
    public String getType() {
        return "mysql";
    }

    /**
     * Inicia a thread de leitura do MySQL Binlog. Se um offset prévio for encontrado, inicia a leitura a partir dele.
     * Caso contrário, executa um snapshot seguro para garantir que nenhum dado seja perdido, e depois começa a ler o Binlog em tempo real.
     * A thread é nomeada com o ID do conector para facilitar a identificação em logs e monitoramento.
     * Em caso de falha crítica (ex: perda de conexão), a thread é encerrada e o status é atualizado para permitir reinício seguro.
     * Observação: O snapshot seguro é realizado utilizando um bloqueio global de leitura (FLUSH TABLES WITH READ LOCK) para garantir consistência, e é recomendado que o banco de dados seja configurado para permitir conexões de leitura durante esse processo para minimizar o impacto na produção.
     */
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

    /**
     * Encerra a thread de leitura do MySQL Binlog e desconecta o cliente. O status é atualizado para permitir reinício seguro.
     * Observação: Em caso de falha ao desconectar, o erro é logado, mas a thread é considerada parada para permitir reinício. O cliente do Binlog é projetado para lidar com reconexões automáticas, então em muitos casos a falha ao desconectar pode ser recuperada na próxima tentativa de conexão.
     */
    @Override
    public void stop() {
        if (!running.getAndSet(false)) return;
        try {
            if (client != null) client.disconnect();
        } catch (Exception e) {
            log.warn("Erro ao desconectar MySQL Binlog", e);
        }
    }

    /**
     * Executa um snapshot seguro do banco de dados MySQL caso nenhum offset prévio seja encontrado. O processo envolve:
//     * 1. Conexão ao banco de dados utilizando JDBC
     * 2. Aplicação de um bloqueio global de leitura (FLUSH TABLES WITH READ LOCK) para garantir consistência durante o snapshot
     * 3. Captura da posição atual do Binlog para garantir que a leitura em tempo real comece a partir do ponto correto após o snapshot
     * 4. Iteração por todas as tabelas do banco de dados e leitura de seus dados, publicando eventos de leitura (READ) para cada registro encontrado
     * 5. Liberação do bloqueio global para permitir que a produção continue normalmente
     * 6. Publicação de eventos de início (BEGIN) e commit (COMMIT) para delimitar a transação do snapshot, utilizando um ID de transação fictício para diferenciar dos eventos do Binlog
     * @throws Exception
     */
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

    /**
     * Extrai os valores das colunas do ResultSet e os mapeia para um Map<String, Object> onde a chave é o nome da coluna e o valor é o valor correspondente do registro. A ordem das colunas é determinada pela lista de nomes de colunas fornecida, que deve corresponder à ordem dos dados no ResultSet.
     * @param rs ResultSet contendo os dados do registro atual
     * @param columnNames Lista de nomes de colunas na ordem correta para mapear os valores do ResultSet
     * @return Map<String, Object> onde a chave é o nome da coluna e o valor é o valor correspondente do registro
     * @throws SQLException em caso de erro ao acessar os dados do ResultSet
     */
    private Map<String, Object> extrairColunasDoResultSet(ResultSet rs, List<String> columnNames) throws SQLException {
        Map<String, Object> columns = new LinkedHashMap<>();
        for (int i = 0; i < columnNames.size(); i++) {
            columns.put(columnNames.get(i), rs.getString(i + 1));
        }
        return columns;
    }

    /**
     * Manipula os eventos recebidos do MySQL Binlog, identificando o tipo de evento e processando-o de acordo. Para eventos de mapeamento de tabela (TableMapEventData), atualiza o cache de mapeamento de tabelas. Para eventos de escrita (WriteRowsEventData), atualização (UpdateRowsEventData) e exclusão (DeleteRowsEventData), extrai os dados relevantes e publica eventos de mudança correspondentes (INSERT, UPDATE, DELETE) para cada registro afetado. Para eventos de transação (XidEventData e QueryEventData com COMMIT), publica um evento de commit
     * @param event Evento recebido do MySQL Binlog, contendo informações sobre a operação realizada no banco de dados
     */
    private void handleEvent(Event event) {
        EventHeaderV4 header = event.getHeader();
        EventData data = event.getData();

        if (client.getBinlogFilename() == null) return;

        String binlogFile = client.getBinlogFilename();
        String binlogPos = String.valueOf(header.getPosition());
        String offsetCoordinates = binlogFile + ":" + binlogPos;

        String txId = "tx-" + header.getServerId() + "-" + header.getTimestamp();

        if (data instanceof TableMapEventData tableData) {
            String db = tableData.getDatabase();
            String table = tableData.getTable();
            log.trace("Mapeando Tabela {}: {}.{}", tableData.getTableId(), db, table);
            tableMap.put(tableData.getTableId(), db + "." + table);

            ChangeEvent beginEvent = new ChangeEvent(UUID.randomUUID(), OperationType.BEGIN, Instant.now(),
                    createMetadata(db, "system", "transaction", txId, offsetCoordinates), null, null);
            useCase.process(beginEvent);
        }

        else if (data instanceof WriteRowsEventData writeData) {
            processRowEvent(writeData.getTableId(), OperationType.INSERT, null, writeData.getRows(), txId, offsetCoordinates);
        }
        else if (data instanceof UpdateRowsEventData updateData) {
            for (Map.Entry<Serializable[], Serializable[]> row : updateData.getRows()) {
                processRowEvent(updateData.getTableId(), OperationType.UPDATE, row.getKey(),
                        Collections.singletonList(row.getValue()), txId, offsetCoordinates);
            }
        }
        else if (data instanceof DeleteRowsEventData deleteData) {
            processRowEvent(deleteData.getTableId(), OperationType.DELETE, null, deleteData.getRows(), txId, offsetCoordinates);
        }

        else if (data instanceof XidEventData) {
            log.trace("Sinal de XID (COMMIT) recebido no MySQL.");
            ChangeEvent commitEvent = new ChangeEvent(UUID.randomUUID(), OperationType.COMMIT, Instant.now(),
                    createMetadata(database, "system", "transaction", txId, offsetCoordinates), null, null);
            useCase.process(commitEvent);
        }
        else if (data instanceof QueryEventData queryData) {
            String query = queryData.getSql().trim().toUpperCase();
            if ("COMMIT".equals(query)) {
                log.trace("Sinal de Query(COMMIT) recebido no MySQL.");
                ChangeEvent commitEvent = new ChangeEvent(UUID.randomUUID(), OperationType.COMMIT, Instant.now(),
                        createMetadata(database, "system", "transaction", txId, offsetCoordinates), null, null);
                useCase.process(commitEvent);
            }
        }
    }

    /**
     * Processa os eventos de linha (INSERT, UPDATE, DELETE) recebidos do MySQL Binlog, mapeando os dados para o formato esperado pelo domínio e publicando eventos de mudança correspondentes. Para eventos de atualização (UPDATE), tanto os dados "antes" quanto "depois" são mapeados para permitir que o domínio identifique as mudanças específicas. Para eventos de exclusão (DELETE), os dados "antes" são mapeados para fornecer contexto sobre o registro que foi removido. O método utiliza o cache de esquema para obter os nomes das colunas e garantir que os dados sejam mapeados corretamente, mesmo que a estrutura da tabela seja alterada ao longo do tempo.
     * @param tableId ID da tabela afetada, utilizado para identificar o nome da tabela e do banco de dados a partir do cache de mapeamento
     * @param op Tipo de operação (INSERT, UPDATE, DELETE) que ocorreu no banco de dados
     * @param beforeArray Array de valores "antes" para eventos de atualização (UPDATE) e exclusão (DELETE), ou null para eventos de inserção (INSERT)
     * @param rows Lista de arrays de valores "depois" para eventos de inserção (INSERT) e atualização (UPDATE), ou null para eventos de exclusão (DELETE)
     * @param txId ID da transação associada ao evento.
     * @param offset String representando a posição do evento no Binlog, utilizado para rastreamento e armazenamento de offset
     */
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

    /**
     * Mapeia os valores de um array de dados do Binlog para um Map<String, Object> onde a chave é o nome da coluna e o valor é o valor correspondente do registro. A ordem dos valores no array é determinada pela estrutura da tabela no MySQL, e os nomes das colunas são obtidos a partir do cache de esquema para garantir que os dados sejam mapeados corretamente, mesmo que a estrutura da tabela seja alterada ao longo do tempo. Se o número de valores no array exceder o número de colunas conhecidas, as colunas adicionais serão nomeadas como "col_0", "col_1", etc. para garantir que todos os dados sejam capt
     * @param columnNames Lista de nomes de colunas na ordem correta para mapear os valores do array do Binlog
     * @param row Array de valores do Binlog representando os dados de um registro, onde a ordem dos valores corresponde à ordem das colunas na tabela do MySQL
     * @return Map<String, Object> onde a chave é o nome da coluna e o valor é o valor correspondente do registro, mapeado a partir do array de dados do Binlog
     */
    private Map<String, Object> mapColumns(List<String> columnNames, Serializable[] row) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < row.length; i++) {
            String colName = (i < columnNames.size()) ? columnNames.get(i) : "col_" + i;
            map.put(colName, (row[i] != null) ? row[i].toString() : null);
        }
        return map;
    }

    /**
     * Cria um objeto SourceMetadata contendo informações sobre a origem do evento de mudança, incluindo o ID do conector, tipo de banco de dados, nome do banco de dados, esquema
     * @param db Nome do banco de dados onde o evento ocorreu, utilizado para identificar a origem do evento e fornecer contexto para o processamento no domínio
     * @param schema Nome do esquema onde o evento ocorreu, utilizado para identificar a origem do evento e fornecer contexto para o processamento no domínio
     * @param table Nome da tabela onde o evento ocorreu, utilizado para identificar a origem do evento e fornecer contexto para o processamento no domínio
     * @param txId ID da transação associada ao evento
     * @param offsetString Representa a posição do evento no Binlog, utilizado para rastreamento e armazenamento de offset
     * @return SourceMetadata contendo informações sobre a origem do evento de mudança, incluindo o ID do conector, tipo de banco de dados, nome do banco de dados, esquema, tabela, ID da transação e posição no Binlog
     */
    private SourceMetadata createMetadata(String db, String schema, String table, String txId, String offsetString) {
        return new SourceMetadata(connectorId, "mysql", db, schema, table, txId, Map.of("binlog_pos", offsetString));
    }
}
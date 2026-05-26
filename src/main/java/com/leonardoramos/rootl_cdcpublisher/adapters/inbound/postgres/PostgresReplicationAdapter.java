package com.leonardoramos.rootl_cdcpublisher.adapters.inbound.postgres;

import com.leonardoramos.rootl_cdcpublisher.application.ports.inbound.ChangeLogConnector;
import com.leonardoramos.rootl_cdcpublisher.application.ports.outbound.OffsetStorePort;
import com.leonardoramos.rootl_cdcpublisher.application.usecases.ProcessChangeEventUseCase;
import com.leonardoramos.rootl_cdcpublisher.domain.model.ChangeEvent;
import com.leonardoramos.rootl_cdcpublisher.domain.model.OperationType;
import com.leonardoramos.rootl_cdcpublisher.domain.model.SourceMetadata;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * PostgresReplicationAdapter é uma implementação do conector de logs de mudança (CDC) para bancos de dados PostgreSQL. Ele utiliza a API de replicação lógica do PostgreSQL para capturar mudanças em tempo real, processar os eventos e gerenciar o estado de offset para garantir a continuidade da captura mesmo após reinicializações.
 * O adaptador é projetado para ser resiliente, lidando com falhas de conexão e erros de forma robusta, e inclui um mecanismo de snapshot seguro para garantir que nenhum dado seja perdido durante a inicialização. Ele também integra métricas para monitorar o status do conector.
 */
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

    /**
     * Inicializa o conector de replicação do PostgreSQL com as configurações necessárias, casos de uso e armazenamento de offset. Esta etapa é crucial para configurar a conexão com o banco de dados, preparar o decodificador de mensagens e registrar as métricas para monitoramento.
     * @param connectorId Id do conector, utilizado para logs e métricas
     * @param config Configurações específicas do conector (credenciais, tópicos, etc)
     * @param useCase Casos de uso do domínio para processar os eventos
     * @param offsetStore Armazenamento de offset para garantir processamento idempotente e reinício seguro
     * @param meterRegistry Registro de métricas para monitorar o conector
     */
    @Override
    public void initialize(String connectorId, Properties config, ProcessChangeEventUseCase useCase, OffsetStorePort offsetStore, MeterRegistry meterRegistry) {
        this.connectorId = connectorId;
        this.jdbcUrl = config.getProperty("jdbcUrl");
        this.user = config.getProperty("user");
        this.password = config.getProperty("password");
        this.slotName = config.getProperty("slotName");
        this.databaseName = config.getProperty("database");
        this.useCase = useCase;
        this.offsetStore = offsetStore;
        this.decoder = new PgOutputDecoder(this.connectorId, this.databaseName);

        Gauge.builder("cdc.connector.status", this.running, r -> r.get() ? 1.0 : 0.0)
                .tag("connector", connectorId)
                .tag("type", getType())
                .description("Status de execução da thread do conector")
                .register(meterRegistry);

        log.info("Conector '{}' inicializado e métricas registradas.", connectorId);
    }

    /**
     * Retorna o tipo do conector, utilizado para identificação e registro de métricas. Este método é importante para diferenciar entre diferentes tipos de conectores em um ambiente com múltiplas fontes de dados.
     * @return "postgresql"
     */
    @Override
    public String getType() {
        return "postgresql";
    }

    /**
     * Inicia a captura de logs de mudança do PostgreSQL. Este método é responsável por iniciar a thread de trabalho que irá estabelecer a conexão de replicação, ler os eventos em tempo real e processá-los usando os casos de uso do domínio. Ele também lida com a lógica de reconexão em caso de falhas e garante que os recursos sejam liberados corretamente ao parar o conector.
     */
    @Override
    public void start() {
        if (running.getAndSet(true)) return;
        workerThread = new Thread(this::replicationLoop, "worker-" + connectorId);
        workerThread.start();
        log.info("Adapter [{}] iniciado com sucesso.", getType());
    }

    /**
     * Interrompe a captura de logs de mudança do PostgreSQL. Este método é responsável por sinalizar a thread de trabalho para parar, fechar a conexão de replicação e garantir que todos os recursos sejam liberados de forma limpa. Ele também lida com possíveis exceções durante o processo de encerramento para evitar vazamentos de recursos
     */
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

    /**
     * Loop principal de replicação que estabelece a conexão de replicação lógica com o PostgreSQL, lê os eventos em tempo real e os processa usando os casos de uso do domínio. Ele também lida com a lógica de reconexão em caso de falhas e garante que os offsets sejam gerenciados corretamente para evitar perda de dados ou processamento duplicado.
     * O método inclui um mecanismo de snapshot seguro para garantir que, na ausência de offsets salvos, o conector possa capturar o estado atual do banco de dados antes de começar a ler os eventos em tempo real. Ele também implementa uma lógica robusta de tratamento de erros para lidar com falhas de conexão e erros críticos, garantindo a resiliência do conector.
     * O loop continua executando enquanto o conector estiver em execução, e inclui pausas estratégicas para evitar sobrecarga do sistema em caso de falhas temporárias. Ele também verifica se os erros são fatais (como problemas de permissão ou configuração) e, nesse caso, interrompe o conector para evitar tentativas de reconexão inúteis.
     */
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
                if (isFatalError(e)){
                    log.error("Erro fatal detectado no stream. Verifique as configurações e permissões do banco.", e);
                    running.set(false);
                    break;
                }
                log.error("Falha crítica detectada no stream. Tentando reconectar em 5 segundos...", e);
                try {
                    Thread.sleep(5000L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * Executa um snapshot seguro do banco de dados caso nenhum offset seja encontrado no armazenamento. Este método é crucial para garantir que, na primeira execução do conector ou após uma limpeza de offsets, o estado atual do banco de dados seja capturado e processado antes de começar a ler os eventos em tempo real. Ele estabelece uma conexão JDBC tradicional, inicia uma transação com isolamento adequado para garantir a consistência dos dados, e lê todas as tabelas publicadas na publicação de CDC configurada. Para cada registro lido, ele cria um evento de mudança com o tipo de operação "READ" e o processa usando os casos de uso do domínio. Após processar todas as tabel
     * @throws SQLException Se ocorrer um erro ao acessar o banco de dados durante o processo de snapshot. O método garante que, em caso de falha, a transação seja revertida para evitar qualquer impacto no banco de dados. Ele também registra informações detalhadas sobre o progresso do snapshot, incluindo o número total de registros processados e o LSN base utilizado para o snapshot.
     */
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

    /**
     * Extrai as colunas e seus valores de um ResultSet, mantendo a tipagem como string para garantir consistência com o decodificador de mensagens do PostgreSQL. Este método é utilizado durante o processo de snapshot para criar um mapa de colunas e valores que será incluído nos eventos de mudança do tipo "READ". Ele utiliza o metadata do ResultSet para iterar sobre as colunas e extrair seus valores, garantindo que mesmo tipos complexos sejam representados como strings para evitar problemas de serialização ou inconsistências na representação dos dados.
     * @param rs O ResultSet do qual as colunas e seus valores serão extraídos. O método assume que o ResultSet está posicionado na linha correta para leitura dos dados.
     * @return Um mapa contendo os nomes das colunas como chaves e seus valores correspondentes como strings. Este mapa será utilizado para criar os eventos de mudança durante o processo de snapshot.
     * @throws SQLException Se ocorrer um erro ao acessar os dados do ResultSet. O método propaga a exceção para que o processo de snapshot possa lidar com falhas de forma adequada
     */
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

    /**
     * Verifica se a exceção capturada é um erro fatal relacionado à configuração, permissões ou estado do banco de dados que impediria o conector de funcionar corretamente. Este método é utilizado para identificar erros que não podem ser resolvidos com tentativas de reconexão, como falta de permissões para o slot de replicação, slot inexistente, ou problemas de autenticação.
     * Ele percorre a cadeia de causas da exceção para identificar se a causa raiz é um PSQLException com um SQLState específico que indica um erro fatal. Se um erro fatal for detectado, o método retorna true, indicando que o conector deve ser interrompido para evitar tentativas de reconexão inúteis. Caso contrário, ele retorna false, permitindo que o conector tente se recuperar de falhas temporárias.
     * @param e A exceção capturada durante a execução do loop de replicação.
     * @return true se a exceção for considerada um erro fatal que requer a interrupção do conector, ou false se a exceção for considerada recuperável e permitir tentativas de reconexão. Os erros fatais incluem, mas não se limitam a, problemas de permissão, slot de replicação inexistente, ou falhas de autenticação. A identificação correta desses erros é crucial para garantir a resiliência do conector e evitar ciclos de reconexão inúteis.
     */
    private boolean isFatalError(Exception e) {
        Throwable rootCause = e;
        while (rootCause.getCause() != null && rootCause != rootCause.getCause()) {
            rootCause = rootCause.getCause();
        }

        if (rootCause instanceof PSQLException psqlEx) {
            String state = psqlEx.getSQLState();
            if (state == null) return false;

            return state.equals("42501") ||
                    state.equals("42704") ||
                    state.equals("55006") ||
                    state.equals("3D000") ||
                    state.equals("28P01");
        }
        return false;
    }
}
package com.leonardoramos.rootl_cdcpublisher.domain.services;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.leonardoramos.rootl_cdcpublisher.domain.model.ChangeEvent;
import jakarta.annotation.PreDestroy;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Buffer de transações persistido em disco usando RocksDB.
 * <p>
 * Garante que eventos de transações em andamento sobrevivam a crashes e reinicializações
 * da aplicação. Cada evento é armazenado com uma chave no formato {@code transactionId:sequencial},
 * permitindo buscas e exclusões eficientes por prefixo (prefix seek).
 * </p>
 * <p>
 * O ciclo de vida do RocksDB é gerenciado pelo Spring: o banco é aberto na inicialização
 * do bean e fechado de forma segura via {@link PreDestroy} durante o shutdown da aplicação.
 * </p>
 */
@Component
public class TransactionBuffer {

    private static final Logger log = LoggerFactory.getLogger(TransactionBuffer.class);
    private static final String KEY_SEPARATOR = ":";

    private final RocksDB db;
    private final Options dbOptions;
    private final ObjectMapper objectMapper;

    /**
     * Sequenciais por transação, garantindo ordenação FIFO dos eventos dentro de cada transação.
     */
    private final ConcurrentHashMap<String, AtomicLong> sequenceCounters = new ConcurrentHashMap<>();

    /**
     * Contador global de eventos no buffer, mantido em memória para leituras rápidas de métricas.
     */
    private final AtomicLong totalEventsCount = new AtomicLong(0);

    /**
     * Inicializa o TransactionBuffer abrindo (ou criando) o banco RocksDB no diretório configurado.
     * Também reconstrói o contador de eventos a partir dos dados já persistidos, para que o valor
     * de métricas seja correto após um restart.
     *
     * @param bufferDir diretório onde os arquivos do RocksDB serão armazenados.
     * @throws RuntimeException se não for possível abrir o banco ou criar o diretório.
     */
    public TransactionBuffer(@Value("${cdc.buffer-dir:./data/transaction-buffer}") String bufferDir) {
        RocksDB.loadLibrary();

        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());

        try {
            Path dbPath = Paths.get(bufferDir);
            Files.createDirectories(dbPath);

            this.dbOptions = new Options()
                    .setCreateIfMissing(true)
                    .useFixedLengthPrefixExtractor(0) // sem prefix extractor fixo; usaremos seek manual
                    .setAllowConcurrentMemtableWrite(true);

            this.db = RocksDB.open(dbOptions, dbPath.toAbsolutePath().toString());

            long recoveredCount = rebuildCountersFromDisk();
            log.info("TransactionBuffer inicializado com RocksDB em '{}'. Eventos recuperados do disco: {}",
                    dbPath.toAbsolutePath(), recoveredCount);

        } catch (RocksDBException | IOException e) {
            throw new RuntimeException("Falha ao inicializar o RocksDB para o TransactionBuffer", e);
        }
    }

    /**
     * Adiciona um evento ao buffer persistente associado à transação.
     * O evento é serializado em JSON e armazenado no RocksDB com a chave {@code transactionId:sequencial}.
     *
     * @param transactionId identificador da transação à qual o evento pertence.
     * @param event         evento de mudança a ser adicionado ao buffer.
     * @throws RuntimeException se ocorrer erro na serialização ou na escrita no RocksDB.
     */
    public void addEvent(String transactionId, ChangeEvent event) {
        try {
            long seq = sequenceCounters
                    .computeIfAbsent(transactionId, k -> new AtomicLong(0))
                    .getAndIncrement();

            String key = buildKey(transactionId, seq);
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = objectMapper.writeValueAsBytes(event);

            db.put(keyBytes, valueBytes);
            totalEventsCount.incrementAndGet();

            if (seq > 0 && seq % 50_000 == 0) {
                log.info("Transação {} em andamento: {} eventos acumulados no buffer.", transactionId, seq);
            }

            log.debug("Evento adicionado ao buffer RocksDB: chave={}", key);
        } catch (RocksDBException | JsonProcessingException e) {
            throw new RuntimeException(
                    "Falha ao adicionar evento ao buffer para a transação " + transactionId, e);
        }
    }

    /**
     * Recupera e remove todos os eventos associados à transação, indicando que ela foi confirmada (COMMIT).
     * Os eventos são lidos em ordem de inserção e deletados atomicamente usando {@link WriteBatch}.
     *
     * @param transactionId identificador da transação a ser confirmada.
     * @return lista ordenada de eventos associados à transação; lista vazia se não houver eventos.
     * @throws RuntimeException se ocorrer erro de leitura, deserialização ou deleção no RocksDB.
     */
    public List<ChangeEvent> commit(String transactionId) {
        List<ChangeEvent> events = new ArrayList<>();
        List<byte[]> keysToDelete = new ArrayList<>();

        byte[] prefix = buildPrefix(transactionId);

        try (RocksIterator iterator = db.newIterator()) {
            iterator.seek(prefix);
            while (iterator.isValid()) {
                byte[] keyBytes = iterator.key();
                String key = new String(keyBytes, StandardCharsets.UTF_8);

                if (!key.startsWith(transactionId + KEY_SEPARATOR)) {
                    break;
                }

                ChangeEvent event = objectMapper.readValue(iterator.value(), ChangeEvent.class);
                events.add(event);
                keysToDelete.add(keyBytes);

                if (events.size() % 50_000 == 0) {
                    log.info("Commit da transação {}: {} eventos lidos do disco até o momento...",
                            transactionId, events.size());
                }

                iterator.next();
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    "Falha ao deserializar eventos da transação " + transactionId, e);
        }

        if (!keysToDelete.isEmpty()) {
            deleteKeys(keysToDelete);
            totalEventsCount.addAndGet(-keysToDelete.size());
        }

        sequenceCounters.remove(transactionId);

        if (events.size() > 10_000) {
            log.info("Commit da transação {}: {} eventos recuperados e removidos do buffer (transação grande).",
                    transactionId, events.size());
        } else {
            log.debug("Commit da transação {}: {} eventos recuperados e removidos do buffer.",
                    transactionId, events.size());
        }
        return events;
    }

    /**
     * Remove todos os eventos associados à transação do buffer (ROLLBACK), descartando-os sem retorná-los.
     *
     * @param transactionId identificador da transação a ser abortada.
     * @throws RuntimeException se ocorrer erro na deleção dos registros no RocksDB.
     */
    public void rollback(String transactionId) {
        byte[] prefixStart = buildPrefix(transactionId);
        // Calcula o end-key para deleteRange: incrementa o último byte do prefixo
        byte[] prefixEnd = buildPrefixEnd(transactionId);

        // Conta os eventos antes de deletar, para log e métricas
        long deletedCount = 0;
        try (RocksIterator iterator = db.newIterator()) {
            iterator.seek(prefixStart);
            while (iterator.isValid()) {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);
                if (!key.startsWith(transactionId + KEY_SEPARATOR)) {
                    break;
                }
                deletedCount++;
                iterator.next();
            }
        }

        if (deletedCount > 0) {
            try {
                db.deleteRange(prefixStart, prefixEnd);
            } catch (RocksDBException e) {
                throw new RuntimeException(
                        "Falha ao executar deleteRange para a transação " + transactionId, e);
            }
            totalEventsCount.addAndGet(-deletedCount);
        }

        sequenceCounters.remove(transactionId);

        log.info("Buffer limpo para transação abortada: {}. {} eventos descartados via deleteRange.",
                transactionId, deletedCount);
    }

    /**
     * Retorna o número total de eventos atualmente retidos no buffer, aguardando confirmação (COMMIT).
     * O valor é mantido em memória via contador atômico, evitando operações de I/O no RocksDB.
     *
     * @return número total de eventos no buffer.
     */
    public double getTotalEventsCount() {
        return totalEventsCount.get();
    }

    /**
     * Fecha o banco RocksDB e libera os recursos associados de forma segura.
     * Chamado automaticamente pelo Spring durante o shutdown da aplicação.
     */
    @PreDestroy
    public void close() {
        log.info("Fechando o RocksDB do TransactionBuffer...");
        if (db != null) {
            db.close();
        }
        if (dbOptions != null) {
            dbOptions.close();
        }
        log.info("RocksDB do TransactionBuffer fechado com sucesso.");
    }

    // ==================== Métodos Privados ====================

    /**
     * Constrói a chave de armazenamento no formato {@code transactionId:sequencial}.
     */
    private String buildKey(String transactionId, long sequence) {
        return transactionId + KEY_SEPARATOR + String.format("%020d", sequence);
    }

    /**
     * Constrói o prefixo de busca no formato {@code transactionId:} em bytes UTF-8.
     */
    private byte[] buildPrefix(String transactionId) {
        return (transactionId + KEY_SEPARATOR).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Constrói o end-key para operações de {@code deleteRange()}, incrementando o último byte
     * do separador para cobrir todo o range de chaves da transação.
     */
    private byte[] buildPrefixEnd(String transactionId) {
        byte[] prefix = buildPrefix(transactionId);
        byte[] end = new byte[prefix.length];
        System.arraycopy(prefix, 0, end, 0, prefix.length);
        end[end.length - 1]++;
        return end;
    }

    /**
     * Deleta um conjunto de chaves atomicamente usando {@link WriteBatch} para garantir
     * consistência em caso de falha parcial.
     */
    private void deleteKeys(List<byte[]> keys) {
        try (WriteOptions writeOptions = new WriteOptions();
             WriteBatch batch = new WriteBatch()) {
            for (byte[] key : keys) {
                batch.delete(key);
            }
            db.write(writeOptions, batch);
        } catch (RocksDBException e) {
            throw new RuntimeException("Falha ao deletar chaves do RocksDB", e);
        }
    }

    /**
     * Reconstrói os contadores de sequência e o contador global de eventos a partir dos dados
     * já persistidos no RocksDB. Necessário após um restart para manter a integridade dos contadores.
     *
     * @return número total de eventos recuperados do disco.
     */
    private long rebuildCountersFromDisk() {
        long count = 0;

        try (RocksIterator iterator = db.newIterator()) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);
                int separatorIdx = key.lastIndexOf(KEY_SEPARATOR);

                if (separatorIdx > 0) {
                    String txId = key.substring(0, separatorIdx);
                    long seq = Long.parseLong(key.substring(separatorIdx + 1));

                    sequenceCounters.computeIfAbsent(txId, k -> new AtomicLong(0))
                            .accumulateAndGet(seq + 1, Math::max);
                }

                count++;
                iterator.next();
            }
        }

        totalEventsCount.set(count);
        return count;
    }
}

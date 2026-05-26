package com.leonardoramos.rootl_cdcpublisher.adapters.outbound.offsets;

import com.leonardoramos.rootl_cdcpublisher.application.ports.outbound.OffsetStorePort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * Adapter para armazenar e recuperar offsets usando o sistema de arquivos local.
 * Cada conector terá um arquivo separado para armazenar seu offset, identificado pelo nome do conector.
 * O offset é salvo como uma string simples (LSN) no arquivo correspondente.
 */
public class FileOffsetStoreAdapter implements OffsetStorePort {

    private static final Logger log = LoggerFactory.getLogger(FileOffsetStoreAdapter.class);
    private final Path storageDirectory;

    public FileOffsetStoreAdapter(String directoryPath) {
        this.storageDirectory = Paths.get(directoryPath);
        try {
            Files.createDirectories(storageDirectory);
        } catch (IOException e) {
            throw new java.io.UncheckedIOException("Não foi possível criar o diretório de offsets", e);
        }
    }

    /**
     * Salva o offset (LSN) para um conector específico em um arquivo no sistema de arquivos local.
     * @param connectorId O identificador do conector para o qual o offset está sendo salvo.
     * @param lsn O offset (LSN) a ser salvo, representado como uma string. Este valor será escrito em um arquivo nomeado com o ID do conector.
     */
    @Override
    public void save(String connectorId, String lsn) {
        Path offsetFile = storageDirectory.resolve(connectorId + ".offset");
        try {
            Files.writeString(offsetFile, lsn);
            log.debug("Offset guardado com sucesso para o conector {}: {}", connectorId, lsn);
        } catch (IOException e) {
            log.error("Erro ao gravar o arquivo de offset para o conector {}", connectorId, e);
        }
    }

    /**
     * Carrega o offset (LSN) para um conector específico a partir de um arquivo no sistema de arquivos local.
     * @param connectorId O identificador do conector para o qual o offset está sendo carregado. O método tentará ler o arquivo nomeado com o ID do conector para obter o offset salvo.
     * @return Um Optional contendo o offset (LSN) como uma string, se o arquivo existir e contiver um valor válido. Se o arquivo não existir ou estiver vazio, retorna um Optional vazio, indicando que o conector deve iniciar a leitura do log a partir do ponto atual.
     */
    @Override
    public Optional<String> load(String connectorId) {
        Path offsetFile = storageDirectory.resolve(connectorId + ".offset");
        if (!Files.exists(offsetFile)) {
            log.info("Nenhum offset anterior encontrado para o conector {}. Iniciando do ponto atual do log.", connectorId);
            return Optional.empty();
        }
        try {
            String lsn = Files.readString(offsetFile).trim();
            return lsn.isEmpty() ? Optional.empty() : Optional.of(lsn);
        } catch (IOException e) {
            log.error("Erro ao ler o arquivo de offset para o conector {}", connectorId, e);
            return Optional.empty();
        }
    }
}

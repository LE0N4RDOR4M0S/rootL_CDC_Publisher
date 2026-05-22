package com.leonardoramos.rootl_cdcpublisher.adapters.outbound.offsets;

import com.leonardoramos.rootl_cdcpublisher.application.ports.outbound.OffsetStorePort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

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

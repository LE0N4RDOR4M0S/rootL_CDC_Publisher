package com.leonardoramos.rootl_cdcpublisher.application.ports.outbound;

import java.util.Optional;

public interface OffsetStorePort {
    void save(String connectorId, String lsn);

    Optional<String> load(String connectorId);
}

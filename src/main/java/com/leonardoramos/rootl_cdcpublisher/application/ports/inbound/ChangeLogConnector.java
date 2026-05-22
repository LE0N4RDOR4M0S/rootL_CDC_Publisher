package com.leonardoramos.rootl_cdcpublisher.application.ports.inbound;

import com.leonardoramos.rootl_cdcpublisher.application.ports.outbound.OffsetStorePort;
import com.leonardoramos.rootl_cdcpublisher.application.usecases.ProcessChangeEventUseCase;
import java.util.Properties;

public interface ChangeLogConnector {

    void initialize(String connectorId, Properties config, ProcessChangeEventUseCase useCase, OffsetStorePort offsetStore);

    void start();

    void stop();

    String getType();
}
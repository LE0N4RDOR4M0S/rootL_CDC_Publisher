package com.leonardoramos.rootl_cdcpublisher.application.ports.inbound;

import com.leonardoramos.rootl_cdcpublisher.application.ports.outbound.OffsetStorePort;
import com.leonardoramos.rootl_cdcpublisher.application.usecases.ProcessChangeEventUseCase;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.Properties;

public interface ChangeLogConnector {

    void initialize(String connectorId, Properties config, ProcessChangeEventUseCase useCase, OffsetStorePort offsetStore, MeterRegistry meterRegistry);

    void start();

    void stop();

    String getType();
}
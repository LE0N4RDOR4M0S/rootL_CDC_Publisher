package com.leonardoramos.rootl_cdcpublisher.application.ports.inbound;

/**
 * Contrato para o port de entrada do CDC Engine.
 */
public interface ChangeDataCapturePort {
    void start();

    void stop();
}

package com.leonardoramos.rootl_cdcpublisher.application.ports.outbound;

import com.leonardoramos.rootl_cdcpublisher.domain.model.ChangeEvent;

public interface EventPublisherPort {
    void publish(ChangeEvent event);
    void close();
}

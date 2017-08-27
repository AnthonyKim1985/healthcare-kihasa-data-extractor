package org.bigdatacenter.healthcarekihasadataextractor.rabbitmq;

import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.ExtractionRequest;

public interface RabbitMQReceiver {
    void runReceiver(ExtractionRequest extractionRequest);
}

package org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.response;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class ExtractionResponse implements Serializable {
    private String jobAcceptTime;
    private String jsonForExtractionRequest;
}

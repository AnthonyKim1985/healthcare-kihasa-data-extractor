package org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.task.extraction;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class DataExtractionTask implements Serializable {
    private String dataFileName;
    private String hdfsLocation;
    private String query;
    private String header;
}

package org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.task;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.task.creation.TableCreationTask;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.task.extraction.DataExtractionTask;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class QueryTask implements Serializable {
    private TableCreationTask tableCreationTask;
    private DataExtractionTask dataExtractionTask;
}
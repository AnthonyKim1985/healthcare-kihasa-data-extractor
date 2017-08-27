package org.bigdatacenter.healthcarekihasadataextractor.service;

import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.task.creation.TableCreationTask;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.task.extraction.DataExtractionTask;

public interface RawDataDBService {
    void extractData(DataExtractionTask dataExtractionTask);

    void createTable(TableCreationTask tableCreationTask);
}

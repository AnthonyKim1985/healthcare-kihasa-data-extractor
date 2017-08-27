package org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.task.QueryTask;
import org.bigdatacenter.healthcarekihasadataextractor.domain.transaction.TrRequestInfo;

import java.io.Serializable;
import java.util.List;

@Data
@AllArgsConstructor
public class ExtractionRequest implements Serializable {
    private String databaseName;
    private TrRequestInfo requestInfo;
    private List<QueryTask> queryTaskList;
}

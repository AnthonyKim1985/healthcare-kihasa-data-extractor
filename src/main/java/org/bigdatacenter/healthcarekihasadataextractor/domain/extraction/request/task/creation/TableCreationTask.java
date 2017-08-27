package org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.task.creation;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class TableCreationTask implements Serializable {
    private String dbAndHashedTableName;
    private String query;
}
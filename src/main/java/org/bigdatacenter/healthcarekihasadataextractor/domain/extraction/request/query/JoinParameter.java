package org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.query;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class JoinParameter implements Serializable {
    private String databaseName;
    private String tableName;
    private String projection;
    private String joinKey;
}
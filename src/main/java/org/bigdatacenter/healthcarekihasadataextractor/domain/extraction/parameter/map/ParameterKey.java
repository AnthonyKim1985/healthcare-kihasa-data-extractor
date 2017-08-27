package org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.parameter.map;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class ParameterKey implements Serializable {
    private Integer dataSetYear;
    private String databaseName;
    private String tableName;
    private String header;
}

package org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.parameter.map;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class ParameterValue implements Serializable {
    private Integer columnType;
    private String columnName;
    private String columnValue;
    private String columnOperator;
}

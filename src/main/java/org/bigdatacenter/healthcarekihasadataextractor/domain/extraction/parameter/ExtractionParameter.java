package org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.parameter;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.parameter.info.AdjacentTableInfo;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.parameter.info.ParameterInfo;
import org.bigdatacenter.healthcarekihasadataextractor.domain.transaction.TrRequestInfo;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

@Data
@AllArgsConstructor
public class ExtractionParameter implements Serializable {
    private String databaseName;
    private TrRequestInfo requestInfo;
    private List<ParameterInfo> parameterInfoList;
    private Set<AdjacentTableInfo> adjacentTableInfoSet;
}
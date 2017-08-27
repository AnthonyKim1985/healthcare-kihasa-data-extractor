package org.bigdatacenter.healthcarekihasadataextractor.resolver.query.where;

import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.parameter.map.ParameterValue;

import java.util.List;

public interface WhereClauseBuilder {
    String buildClause(List<ParameterValue> parameterValueList);
}
package org.bigdatacenter.healthcarekihasadataextractor.resolver.query.join;

import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.query.JoinParameter;

import java.util.List;

public interface JoinClauseBuilder {
    String buildClause(List<JoinParameter> joinParameterList);
    String buildClause(JoinParameter sourceJoinParameter, JoinParameter targetJoinParameter);
}

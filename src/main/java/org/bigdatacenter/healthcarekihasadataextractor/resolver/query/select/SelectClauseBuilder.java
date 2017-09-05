package org.bigdatacenter.healthcarekihasadataextractor.resolver.query.select;

public interface SelectClauseBuilder {
    String buildClause(String dbName, String tableName);

    String buildClause(String dbName, String tableName, String projections, Boolean enableDistinct);

    String buildClause(String dbName, String tableName, String projections, String snpRs, String affy5MapNumber);
}
package org.bigdatacenter.healthcarekihasadataextractor.resolver.query.select;

public interface SelectClauseBuilder {
    String buildClause(String dbName, String tableName);

    String buildClause(String dbName, String tableName, String projections, Boolean enableDistinct);

    String buildClause(Integer affy5MapNumber, String snpRs, String sourceDbAndTableName, String targetDbAndTableName);
}
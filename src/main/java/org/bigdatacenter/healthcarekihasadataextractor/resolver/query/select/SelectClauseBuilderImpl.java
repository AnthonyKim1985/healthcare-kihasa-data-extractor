package org.bigdatacenter.healthcarekihasadataextractor.resolver.query.select;

import org.springframework.stereotype.Component;

@Component
public class SelectClauseBuilderImpl implements SelectClauseBuilder {
    @Override
    public String buildClause(String dbName, String tableName) {
        if (dbName == null || dbName.isEmpty())
            throw new NullPointerException("The dbName is either null or empty.");

        if (tableName == null || tableName.isEmpty())
            throw new NullPointerException("The tableName is either null or empty.");

        return String.format("SELECT * FROM %s.%s", dbName, tableName);
    }

    @Override
    public String buildClause(String dbName, String tableName, String projections, Boolean enableDistinct) {
        if (dbName == null || dbName.isEmpty())
            throw new NullPointerException("The dbName is either null or empty.");

        if (tableName == null || tableName.isEmpty())
            throw new NullPointerException("The tableName is either null or empty.");

        if (projections == null || projections.isEmpty())
            throw new NullPointerException("The projections is either null or empty.");

        if (enableDistinct)
            return String.format("SELECT DISTINCT %s FROM %s.%s", projections, dbName, tableName);

        return String.format("SELECT %s FROM %s.%s", projections, dbName, tableName);
    }
}
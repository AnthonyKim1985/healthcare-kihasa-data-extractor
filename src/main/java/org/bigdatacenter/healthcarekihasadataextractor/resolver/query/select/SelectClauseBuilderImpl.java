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

    @Override
    public String buildClause(String dbName, String tableName, String projections, String snpRs, Integer affy5MapNumber) {
        if (dbName == null || dbName.isEmpty())
            throw new NullPointerException("The dbName is either null or empty.");

        if (tableName == null || tableName.isEmpty())
            throw new NullPointerException("The tableName is either null or empty.");

        if (projections == null || projections.isEmpty())
            throw new NullPointerException("The projections is either null or empty.");

        if (snpRs == null || snpRs.isEmpty())
            throw new NullPointerException("The snpRs is either null or empty.");

        if (affy5MapNumber == null)
            throw new NullPointerException("The affy5mapNumber is null.");

        return String.format("SELECT %s, substring(snp, 2 * %d - 1, 1) %s_1, substring(snp, 2 * %d, 1) %s_2 FROM %s.%s",
                projections, affy5MapNumber, snpRs, affy5MapNumber, snpRs, dbName, tableName);
    }
}
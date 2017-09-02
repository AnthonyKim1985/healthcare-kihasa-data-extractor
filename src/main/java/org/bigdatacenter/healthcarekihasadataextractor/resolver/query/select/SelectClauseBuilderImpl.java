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
    public String buildClause(Integer affy5MapNumber, String snpRs, String sourceDbAndTableName, String targetDbAndTableName) {
        if (affy5MapNumber == null)
            throw new NullPointerException("The affy5mapNumber is null.");

        if (snpRs == null || snpRs.isEmpty())
            throw new NullPointerException("The snpRs is either null or empty.");

        if (sourceDbAndTableName == null || sourceDbAndTableName.isEmpty())
            throw new NullPointerException("The sourceDbAndTableName is either null or empty.");

        if (targetDbAndTableName == null || targetDbAndTableName.isEmpty())
            throw new NullPointerException("The targetDbAndTableName is either null or empty.");

        return String.format("SELECT A.*, substring(B.snp, 2 * %d - 1, 1) %s_1, substring(B.snp, 2 * %d, 1) %s_2 FROM %s A, %s B",
                affy5MapNumber, snpRs, affy5MapNumber, snpRs, sourceDbAndTableName,targetDbAndTableName);
    }
}
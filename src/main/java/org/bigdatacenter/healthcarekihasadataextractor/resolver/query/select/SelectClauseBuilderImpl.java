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
    public String buildClause(String dbName, String tableName, String projections, String snpRs, String affy5MapNumber) {
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

        final StringBuilder selectBuilder = new StringBuilder(String.format("SELECT %s,", projections));
        final String[] snpRsArray = snpRs.split("[,]");
        final String[] affy5MapNumberArray = affy5MapNumber.split("[,]");

        if (snpRsArray.length != affy5MapNumberArray.length)
            throw new RuntimeException("The number of snpRs and the number of affy5MapNumber are not matched.");

        for (int i = 0; i < affy5MapNumberArray.length; i++) {
            String snpRsElement = snpRsArray[i];
            String affy5MapNumberElement = affy5MapNumberArray[i];

            selectBuilder.append(String.format("substring(snp, 2 * %s - 1, 1) %s_1,substring(snp, 2 * %s, 1) %s_2",
                    affy5MapNumberElement, snpRsElement, affy5MapNumberElement, snpRsElement));

            if (i < affy5MapNumberArray.length - 1)
                selectBuilder.append(',');
        }

        selectBuilder.append(String.format(" FROM %s.%s", dbName, tableName));

        return selectBuilder.toString();
    }
}
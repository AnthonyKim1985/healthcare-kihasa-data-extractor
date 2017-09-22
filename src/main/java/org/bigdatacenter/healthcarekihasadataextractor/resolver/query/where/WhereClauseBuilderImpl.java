package org.bigdatacenter.healthcarekihasadataextractor.resolver.query.where;

import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.parameter.map.ParameterValue;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class WhereClauseBuilderImpl implements WhereClauseBuilder {
    @Override
    public String buildClause(List<ParameterValue> parameterValueList) {
        if (parameterValueList == null || parameterValueList.isEmpty())
            throw new NullPointerException("The parameterValueList is either null or empty.");

        try {
            final int parameterValueListSize = parameterValueList.size();
            final StringBuilder whereClauseBuilder = new StringBuilder("WHERE");

            for (int i = 0; i < parameterValueListSize; i++) {
                ParameterValue parameterValue = parameterValueList.get(i);

                final String columnName = parameterValue.getColumnName().toLowerCase();
                final String columnValue = String.format("'%s'", parameterValue.getColumnValue().replaceAll("[,]", "','"));
                final String columnOperator = parameterValue.getColumnOperator();
                whereClauseBuilder.append(getComparisonClause(columnName, columnOperator, columnValue));

                if (i < parameterValueListSize - 1)
                    whereClauseBuilder.append(" AND ");
            }
            return whereClauseBuilder.toString();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    private String getComparisonClause(String columnName, String columnOperator, String columnValue) {
        StringBuilder comparisonClauseBuilder = new StringBuilder();

        switch (columnOperator) {
            case "IN":
                comparisonClauseBuilder.append(String.format(" %s IN (%s)", columnName, columnValue));
                break;
            case "<":
                comparisonClauseBuilder.append(String.format(" %s < %s", columnName, columnValue));
                break;
            case ">":
                comparisonClauseBuilder.append(String.format(" %s > %s", columnName, columnValue));
                break;
            case "<=":
                comparisonClauseBuilder.append(String.format(" %s <= %s", columnName, columnValue));
                break;
            case ">=":
                comparisonClauseBuilder.append(String.format(" %s >= %s", columnName, columnValue));
                break;
            case "<>":
                comparisonClauseBuilder.append(String.format(" %s <> %s", columnName, columnValue));
                break;
            default:
                throw new RuntimeException(String.format("Invalid comparison operator: %s", columnOperator));
        }

        return comparisonClauseBuilder.toString();
    }
}
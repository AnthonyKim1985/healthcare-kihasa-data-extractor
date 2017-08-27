package org.bigdatacenter.healthcarekihasadataextractor.resolver.extraction.parameter;

import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.parameter.ExtractionParameter;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.parameter.info.AdjacentTableInfo;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.parameter.info.ParameterInfo;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.parameter.map.ParameterKey;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.parameter.map.ParameterValue;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.parameter.ExtractionRequestParameter;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class ExtractionRequestParameterResolverImpl implements ExtractionRequestParameterResolver {
    @Override
    public ExtractionRequestParameter buildRequestParameter(ExtractionParameter extractionParameter) {
        final List<ParameterInfo> parameterInfoList = extractionParameter.getParameterInfoList();
        final Set<AdjacentTableInfo> adjacentTableInfoSet = extractionParameter.getAdjacentTableInfoSet();

        final Map<Integer/* Year */, Set<ParameterKey>> yearJoinKeyMap = new HashMap<>();
        final Map<Integer/* Year */, Map<ParameterKey, List<ParameterValue>>> yearParameterMap = new HashMap<>();
        final Map<Integer/* Year */, Set<AdjacentTableInfo>> yearAdjacentTableInfoMap = new HashMap<>();

        //
        // TODO: Convert ParameterInfo List to yearParameterMap
        //
        for (ParameterInfo parameterInfo : parameterInfoList) {
            final Integer dataSetYear = parameterInfo.getDataSetYear();
            final String databaseName = parameterInfo.getDatabaseName();
            final String tableName = parameterInfo.getTableName();
            final String header = parameterInfo.getHeader();

            final Integer columnType = parameterInfo.getColumnType();
            final String columnName = parameterInfo.getColumnName();
            final String columnValue = parameterInfo.getColumnValue();
            final String columnOperator = parameterInfo.getColumnOperator();

            ParameterKey parameterKey = new ParameterKey(dataSetYear, databaseName, tableName, header);
            ParameterValue parameterValue = new ParameterValue(columnType, columnName, columnValue, columnOperator);

            if (columnType == 1) {
                Set<ParameterKey> joinKeySet = yearJoinKeyMap.get(dataSetYear);

                //noinspection Duplicates
                if (joinKeySet == null) {
                    joinKeySet = new HashSet<>();

                    joinKeySet.add(parameterKey);
                    yearJoinKeyMap.put(dataSetYear, joinKeySet);
                } else {
                    joinKeySet.add(parameterKey);
                }
            }

            List<ParameterValue> parameterValueList;
            Map<ParameterKey, List<ParameterValue>> parameterMap = yearParameterMap.get(dataSetYear);

            if (parameterMap == null) {
                parameterMap = new HashMap<>();
                parameterValueList = new ArrayList<>();

                parameterValueList.add(parameterValue);
                parameterMap.put(parameterKey, parameterValueList);
                yearParameterMap.put(dataSetYear, parameterMap);
            } else {
                parameterValueList = parameterMap.get(parameterKey);

                //noinspection Duplicates
                if (parameterValueList == null) {
                    parameterValueList = new ArrayList<>();

                    parameterValueList.add(parameterValue);
                    parameterMap.put(parameterKey, parameterValueList);
                } else {
                    parameterValueList.add(parameterValue);
                }
            }
        }

        //
        // TODO: Convert AdjacentTableInfo Set to yearAdjacentTableInfoMap
        //
        for (AdjacentTableInfo adjacentTableInfo : adjacentTableInfoSet) {
            final Integer dataSetYear = adjacentTableInfo.getDataSetYear();

            Set<AdjacentTableInfo> yearAdjacentTableInfoMapValue = yearAdjacentTableInfoMap.get(dataSetYear);

            //noinspection Duplicates
            if (yearAdjacentTableInfoMapValue == null) {
                yearAdjacentTableInfoMapValue = new HashSet<>();

                yearAdjacentTableInfoMapValue.add(adjacentTableInfo);
                yearAdjacentTableInfoMap.put(dataSetYear, yearAdjacentTableInfoMapValue);
            } else {
                yearAdjacentTableInfoMapValue.add(adjacentTableInfo);
            }
        }

        return new ExtractionRequestParameter(yearJoinKeyMap, yearParameterMap, yearAdjacentTableInfoMap);
    }
}

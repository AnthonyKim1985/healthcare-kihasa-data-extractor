package org.bigdatacenter.healthcarekihasadataextractor.resolver.extraction;

import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.parameter.ExtractionParameter;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.parameter.info.AdjacentTableInfo;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.parameter.map.ParameterKey;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.parameter.map.ParameterValue;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.ExtractionRequest;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.parameter.ExtractionRequestParameter;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.query.JoinParameter;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.task.QueryTask;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.task.creation.TableCreationTask;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.task.extraction.DataExtractionTask;
import org.bigdatacenter.healthcarekihasadataextractor.domain.transaction.TrRequestInfo;
import org.bigdatacenter.healthcarekihasadataextractor.resolver.extraction.parameter.ExtractionRequestParameterResolver;
import org.bigdatacenter.healthcarekihasadataextractor.resolver.query.join.JoinClauseBuilder;
import org.bigdatacenter.healthcarekihasadataextractor.resolver.query.select.SelectClauseBuilder;
import org.bigdatacenter.healthcarekihasadataextractor.resolver.query.where.WhereClauseBuilder;
import org.bigdatacenter.healthcarekihasadataextractor.util.CommonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class ExtractionRequestResolverImpl implements ExtractionRequestResolver {
    private static final Logger logger = LoggerFactory.getLogger(ExtractionRequestResolverImpl.class);
    private static final String currentThreadName = Thread.currentThread().getName();

    private final SelectClauseBuilder selectClauseBuilder;

    private final WhereClauseBuilder whereClauseBuilder;

    private final JoinClauseBuilder joinClauseBuilder;

    private final ExtractionRequestParameterResolver extractionRequestParameterResolver;

    @Autowired
    public ExtractionRequestResolverImpl(SelectClauseBuilder selectClauseBuilder,
                                         WhereClauseBuilder whereClauseBuilder,
                                         JoinClauseBuilder joinClauseBuilder,
                                         ExtractionRequestParameterResolver extractionRequestParameterResolver) {
        this.selectClauseBuilder = selectClauseBuilder;
        this.whereClauseBuilder = whereClauseBuilder;
        this.joinClauseBuilder = joinClauseBuilder;
        this.extractionRequestParameterResolver = extractionRequestParameterResolver;
    }

    @Override
    @SuppressWarnings("Duplicates")
    public ExtractionRequest buildExtractionRequest(ExtractionParameter extractionParameter) {
        if (extractionParameter == null)
            throw new NullPointerException(String.format("%s - extractionParameter is null.", currentThreadName));

        try {
            final TrRequestInfo requestInfo = extractionParameter.getRequestInfo();
            final Integer dataSetUID = requestInfo.getDataSetUID();
            final String databaseName = extractionParameter.getDatabaseName();
            final String joinCondition = requestInfo.getJoinCondition();
            final Integer joinConditionYear = requestInfo.getJoinConditionYear();

            if (joinConditionYear == null)
                throw new NullPointerException("The joinConditionYear is null value. (must be zero or positive number)");
            else if (joinConditionYear < 0)
                throw new NullPointerException("The joinConditionYear is zero. (must be zero or positive number)");

            final ExtractionRequestParameter extractionRequestParameter = extractionRequestParameterResolver.buildRequestParameter(extractionParameter);

            final Map<Integer/* Year */, Map<ParameterKey, List<ParameterValue>>> yearParameterMap = extractionRequestParameter.getYearParameterMap();
            final Map<Integer/* Year */, Set<ParameterKey>> yearJoinKeyMap = extractionRequestParameter.getYearJoinKeyMap();
            final Map<Integer/* Year */, Set<AdjacentTableInfo>> yearAdjacentTableInfoMap = extractionRequestParameter.getYearAdjacentTableInfoMap();

            final List<QueryTask> queryTaskList = new ArrayList<>();
            final Map<Integer/* Year */, JoinParameter> joinParameterMapForExtraction = new HashMap<>();

            //
            // TODO: 1. 추출 연산을 위한 임시 테이블들을 생성한다.
            //
            for (Integer year : yearParameterMap.keySet()) {
                Map<ParameterKey, List<ParameterValue>> parameterMap = yearParameterMap.get(year);
                Set<ParameterKey> joinTargetKeySet = yearJoinKeyMap.get(year);

                //
                // TODO: 1.1. 조인 대상키가 없으면 해당 연도를 스킵힌다.
                //
                if (joinTargetKeySet == null || joinTargetKeySet.isEmpty())
                    continue;

                //
                // TODO: 1.2. 조인 기준 연도가 있고 (값이 0 보다 크고) 기준연도가 아니라면 스킵한다.
                //
                if (joinConditionYear > 0)
                    if (!Objects.equals(joinConditionYear, year))
                        continue;

                //
                // TODO: 1.3. 임시 테이블 생성 쿼리를 생성한다. (유니크 컬럼을 갖고 있는 테이블만 대상)
                //
                List<JoinParameter> joinParameterList = new ArrayList<>();
                for (ParameterKey parameterKey : parameterMap.keySet()) {
                    final String tableName = parameterKey.getTableName();
                    final String header = parameterKey.getHeader();

                    final String selectClause = selectClauseBuilder.buildClause(databaseName, tableName, header, Boolean.TRUE);
                    final String whereClause = whereClauseBuilder.buildClause(parameterMap.get(parameterKey));
                    final String query = String.format("%s %s", selectClause, whereClause);
                    logger.info(String.format("%s - query: %s", currentThreadName, query));

                    final String extrDbName = String.format("%s_extracted", databaseName);
                    final String extrTableName = String.format("%s_%s", databaseName, CommonUtil.getHashedString(query));
                    final String dbAndHashedTableName = String.format("%s.%s", extrDbName, extrTableName);
                    logger.info(String.format("%s - dbAndHashedTableName: %s", currentThreadName, dbAndHashedTableName));

                    TableCreationTask tableCreationTask = new TableCreationTask(dbAndHashedTableName, query);

                    queryTaskList.add(new QueryTask(tableCreationTask, null));
                    joinParameterList.add(new JoinParameter(extrDbName, extrTableName, joinCondition, joinCondition));
                }

                //
                // TODO: 1.4. 임시 데이블들의 조인 연산을 위한 테이블 생성 쿼리를 생성한다.
                //
                final String joinQuery = joinClauseBuilder.buildClause(joinParameterList);
                logger.info(String.format("%s - joinQuery: %s", currentThreadName, joinQuery));

                final String joinDbName = String.format("%s_join_%s_integrated", databaseName, joinCondition);
                final String joinTableName = String.format("%s_%s", databaseName, CommonUtil.getHashedString(joinQuery));
                final String dbAndHashedTableName = String.format("%s.%s", joinDbName, joinTableName);

                TableCreationTask tableCreationTask = new TableCreationTask(dbAndHashedTableName, joinQuery);
                queryTaskList.add(new QueryTask(tableCreationTask, null));

                JoinParameter joinParameter = new JoinParameter(joinDbName, joinTableName, joinCondition, joinCondition);
                joinParameterMapForExtraction.put(year, joinParameter);
            }

            //
            // TODO: 2. 원시 데이터 셋 테이블과 조인연산 수행을 위한 쿼리 및 데이터 추출 쿼리를 생성한다.
            //
            /* 기준 연도가 주어지지 않았을 때 (비추적) */
            if (joinConditionYear == 0) {
                for (Integer dataSetYear : joinParameterMapForExtraction.keySet()) {
                    final JoinParameter targetJoinParameter = joinParameterMapForExtraction.get(dataSetYear);
                    final Set<AdjacentTableInfo> adjacentTableInfoSet = yearAdjacentTableInfoMap.get(dataSetYear);
                    queryTaskList.addAll(getJoinQueryTasks(adjacentTableInfoSet, targetJoinParameter, databaseName, joinCondition, requestInfo.getDataSetUID()));
                }
            }
            /* 기준 연도가 주어졌을 때 (추적) */
            else {
                final JoinParameter targetJoinParameter = joinParameterMapForExtraction.get(joinConditionYear);
                for (Integer sourceDataSetYear : yearAdjacentTableInfoMap.keySet()) {
                    final Set<AdjacentTableInfo> adjacentTableInfoSet = yearAdjacentTableInfoMap.get(sourceDataSetYear);
                    queryTaskList.addAll(getJoinQueryTasks(adjacentTableInfoSet, targetJoinParameter, databaseName, joinCondition, dataSetUID));
                }
            }

            return new ExtractionRequest(databaseName, requestInfo, queryTaskList);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    private List<QueryTask> getJoinQueryTasks(Set<AdjacentTableInfo> adjacentTableInfoSet, JoinParameter targetJoinParameter, String databaseName, String joinCondition, Integer dataSetUID) {
        List<QueryTask> queryTaskList = new ArrayList<>();

        try {
            for (AdjacentTableInfo adjacentTableInfo : adjacentTableInfoSet) {
                final String tableName = adjacentTableInfo.getTableName();
                final String header = adjacentTableInfo.getHeader();

                JoinParameter sourceJoinParameter = new JoinParameter(databaseName, tableName, header, joinCondition);

                final String joinQuery = joinClauseBuilder.buildClause(sourceJoinParameter, targetJoinParameter);
                final String joinDbName = String.format("%s_join_%s_integrated", databaseName, joinCondition);
                final String joinTableName = String.format("%s_%s", databaseName, CommonUtil.getHashedString(joinQuery));
                final String dbAndHashedTableName = String.format("%s.%s", joinDbName, joinTableName);
                final String extractionQuery = selectClauseBuilder.buildClause(joinDbName, joinTableName, header, Boolean.FALSE);

                TableCreationTask tableCreationTask = new TableCreationTask(dbAndHashedTableName, joinQuery);
                DataExtractionTask dataExtractionTask = new DataExtractionTask(tableName/*Data File Name*/, CommonUtil.getHdfsLocation(dbAndHashedTableName, dataSetUID), extractionQuery, header);

                queryTaskList.add(new QueryTask(tableCreationTask, dataExtractionTask));
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }

        return queryTaskList;
    }
}
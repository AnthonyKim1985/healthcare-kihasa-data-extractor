package org.bigdatacenter.healthcarekihasadataextractor.resolver.extraction;

import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.parameter.ExtractionParameter;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.parameter.map.ParameterKey;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.parameter.map.ParameterValue;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.ExtractionRequest;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.parameter.ExtractionRequestParameter;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.task.QueryTask;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.task.creation.TableCreationTask;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.task.extraction.DataExtractionTask;
import org.bigdatacenter.healthcarekihasadataextractor.domain.transaction.TrRequestInfo;
import org.bigdatacenter.healthcarekihasadataextractor.resolver.extraction.parameter.ExtractionRequestParameterResolver;
import org.bigdatacenter.healthcarekihasadataextractor.resolver.query.select.SelectClauseBuilder;
import org.bigdatacenter.healthcarekihasadataextractor.resolver.query.where.WhereClauseBuilder;
import org.bigdatacenter.healthcarekihasadataextractor.util.CommonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class ExtractionRequestResolverImpl implements ExtractionRequestResolver {
    private static final Logger logger = LoggerFactory.getLogger(ExtractionRequestResolverImpl.class);
    private static final String currentThreadName = Thread.currentThread().getName();

    private final SelectClauseBuilder selectClauseBuilder;

    private final WhereClauseBuilder whereClauseBuilder;

    private final ExtractionRequestParameterResolver extractionRequestParameterResolver;

    @Autowired
    public ExtractionRequestResolverImpl(SelectClauseBuilder selectClauseBuilder,
                                         WhereClauseBuilder whereClauseBuilder,
                                         ExtractionRequestParameterResolver extractionRequestParameterResolver)
    {
        this.selectClauseBuilder = selectClauseBuilder;
        this.whereClauseBuilder = whereClauseBuilder;
        this.extractionRequestParameterResolver = extractionRequestParameterResolver;
    }

    @Override
    public ExtractionRequest buildExtractionRequest(ExtractionParameter extractionParameter) {
        if (extractionParameter == null)
            throw new NullPointerException(String.format("%s - extractionParameter is null.", currentThreadName));

        try {
            final TrRequestInfo requestInfo = extractionParameter.getRequestInfo();
            final Integer dataSetUID = requestInfo.getDataSetUID();
            final String databaseName = extractionParameter.getDatabaseName();

            final ExtractionRequestParameter extractionRequestParameter = extractionRequestParameterResolver.buildRequestParameter(extractionParameter);
            final Map<Integer/* Year */, Map<ParameterKey, List<ParameterValue>>> yearParameterMap = extractionRequestParameter.getYearParameterMap();

            final List<QueryTask> queryTaskList = new ArrayList<>();

            //
            // TODO: 1. 임시 테이블 생성과 데이터 추출을 위한 쿼리를 생성한다.
            //
            for (Integer year : yearParameterMap.keySet()) {
                Map<ParameterKey, List<ParameterValue>> parameterMap = yearParameterMap.get(year);

                for (ParameterKey parameterKey : parameterMap.keySet()) {
                    final String tableName = parameterKey.getTableName();
                    final String header = parameterKey.getHeader();


                }
            }
            return new ExtractionRequest(databaseName, requestInfo, queryTaskList);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }
}
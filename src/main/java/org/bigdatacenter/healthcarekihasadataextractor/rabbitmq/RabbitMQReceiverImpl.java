package org.bigdatacenter.healthcarekihasadataextractor.rabbitmq;

import org.bigdatacenter.healthcarekihasadataextractor.api.caller.DataIntegrationPlatformAPICaller;
import org.bigdatacenter.healthcarekihasadataextractor.api.caller.statistic.StatisticAPICaller;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.ExtractionRequest;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.task.QueryTask;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.task.creation.TableCreationTask;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.task.extraction.DataExtractionTask;
import org.bigdatacenter.healthcarekihasadataextractor.domain.transaction.TrRequestInfo;
import org.bigdatacenter.healthcarekihasadataextractor.resolver.script.ShellScriptResolver;
import org.bigdatacenter.healthcarekihasadataextractor.service.RawDataDBService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.util.List;

@Component
public class RabbitMQReceiverImpl implements RabbitMQReceiver {
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQReceiverImpl.class);
    private static final String currentThreadName = Thread.currentThread().getName();

    private static final int ALL_CONDITIONS_NOT_MET = 0x000000000;
    private static final int DATABASE_NAME_IS_NULL = 0x000000FF;
    private static final int QUERY_TASK_LIST_IS_NULL = 0x0000FF00;
    private static final int REQUEST_INFO_NULL = 0x00FF0000;
    private static final int QUERY_TASK_LIST_IS_EMPTY = 0xFF000000;
    private static final int ALL_CONDITIONS_MET = 0xFFFFFFFF;

    private final ShellScriptResolver shellScriptResolver;

    private final RawDataDBService rawDataDBService;

    private final DataIntegrationPlatformAPICaller dataIntegrationPlatformAPICaller;

    private final StatisticAPICaller statisticAPICaller;

    @Value("${shellscript.path.home}")
    private String homePath;

    @Autowired
    public RabbitMQReceiverImpl(ShellScriptResolver shellScriptResolver, RawDataDBService rawDataDBService, DataIntegrationPlatformAPICaller dataIntegrationPlatformAPICaller, StatisticAPICaller statisticAPICaller) {
        this.shellScriptResolver = shellScriptResolver;
        this.rawDataDBService = rawDataDBService;
        this.dataIntegrationPlatformAPICaller = dataIntegrationPlatformAPICaller;
        this.statisticAPICaller = statisticAPICaller;
    }

    @Override
    public void runReceiver(ExtractionRequest extractionRequest) {
        if (extractionRequest == null) {
            logger.error(String.format("(dataSetUID=null / threadName=%s) - Error occurs at RabbitMQReceiver: extraction request is null", currentThreadName));
            return;
        }

        final int errorCode = checkExtractionRequestValidity(extractionRequest);

        if (errorCode == ALL_CONDITIONS_NOT_MET) {
            final TrRequestInfo requestInfo = extractionRequest.getRequestInfo();
            final Integer dataSetUID = requestInfo.getDataSetUID();

            try {
                final Long jobStartTime = System.currentTimeMillis();
                dataIntegrationPlatformAPICaller.callUpdateJobStartTime(dataSetUID, jobStartTime);
                dataIntegrationPlatformAPICaller.callUpdateProcessState(dataSetUID, DataIntegrationPlatformAPICaller.PROCESS_STATE_CODE_PROCESSING);

                runQueryTask(extractionRequest);
                runArchiveTask(extractionRequest);

                final Long jobEndTime = System.currentTimeMillis();
                dataIntegrationPlatformAPICaller.callUpdateJobEndTime(dataSetUID, jobEndTime);
                dataIntegrationPlatformAPICaller.callUpdateElapsedTime(dataSetUID, (jobEndTime - jobStartTime));
                dataIntegrationPlatformAPICaller.callUpdateProcessState(dataSetUID, DataIntegrationPlatformAPICaller.PROCESS_STATE_CODE_COMPLETED);
            } catch (Exception receiverException) {
                try {
                    dataIntegrationPlatformAPICaller.callUpdateProcessState(dataSetUID, DataIntegrationPlatformAPICaller.PROCESS_STATE_CODE_REJECTED);
                    logger.error(String.format("(dataSetUID=%d / threadName=%s) - Exception occurs in RabbitMQReceiver: %s", dataSetUID, currentThreadName, receiverException.getMessage()));
                    logger.error(String.format("(dataSetUID=%d / threadName=%s) - Bad Extraction Request: %s", dataSetUID, currentThreadName, extractionRequest));
                    receiverException.printStackTrace();
                } catch (Exception platformApiException) {
                    platformApiException.printStackTrace();
                }
            }
        } else {
            try {
                if ((errorCode & REQUEST_INFO_NULL) == REQUEST_INFO_NULL) {
                    logger.error(String.format("(dataSetUID=null / threadName=%s) - Bad Extraction Request: %s", currentThreadName, extractionRequest));
                } else {
                    final Integer dataSetUID = extractionRequest.getRequestInfo().getDataSetUID();
                    dataIntegrationPlatformAPICaller.callUpdateProcessState(dataSetUID, DataIntegrationPlatformAPICaller.PROCESS_STATE_CODE_REJECTED);
                    logger.error(String.format("(dataSetUID=%d / threadName=%s) - Bad Extraction Request: %s", dataSetUID, currentThreadName, extractionRequest));
                }
            } catch (Exception platformException) {
                platformException.printStackTrace();
            }
        }
    }

    private int checkExtractionRequestValidity(ExtractionRequest extractionRequest) {
        int conditionCode = ALL_CONDITIONS_NOT_MET;

        final String databaseName = extractionRequest.getDatabaseName();
        final TrRequestInfo requestInfo = extractionRequest.getRequestInfo();
        final List<QueryTask> queryTaskList = extractionRequest.getQueryTaskList();

        final Boolean isDatabaseNameNull = databaseName == null;
        final Boolean isRequestInfoNull = requestInfo == null;
        final Boolean isQueryTaskListNull = queryTaskList == null;

        if (isDatabaseNameNull) {
            if (isRequestInfoNull)
                logger.error(String.format("(dataSetUID=null / threadName=%s) - Error occurs at RabbitMQReceiver: databaseName is null.", currentThreadName));
            else
                logger.error(String.format("(dataSetUID=%d / threadName=%s) - Error occurs at RabbitMQReceiver: databaseName is null.", requestInfo.getDataSetUID(), currentThreadName));
            conditionCode = conditionCode | DATABASE_NAME_IS_NULL;
        }

        if (isRequestInfoNull) {
            logger.error(String.format("(dataSetUID=null / threadName=%s) - Error occurs at RabbitMQReceiver: requestInfo is null.", currentThreadName));
            conditionCode = conditionCode | REQUEST_INFO_NULL;
        }

        if (isQueryTaskListNull) {
            if (isRequestInfoNull)
                logger.error(String.format("(dataSetUID=null / threadName=%s) - Error occurs at RabbitMQReceiver: queryTaskList is null.", currentThreadName));
            else
                logger.error(String.format("(dataSetUID=%d / threadName=%s) - Error occurs at RabbitMQReceiver: queryTaskList is null.", requestInfo.getDataSetUID(), currentThreadName));
            conditionCode = conditionCode | QUERY_TASK_LIST_IS_NULL;
        } else if (queryTaskList.isEmpty()) {
            if (isRequestInfoNull)
                logger.error(String.format("(dataSetUID=null / threadName=%s) - Error occurs at RabbitMQReceiver: queryTaskList is empty.", currentThreadName));
            else
                logger.error(String.format("(dataSetUID=%d / threadName=%s) - Error occurs at RabbitMQReceiver: queryTaskList is empty.", requestInfo.getDataSetUID(), currentThreadName));
            conditionCode = conditionCode | QUERY_TASK_LIST_IS_EMPTY;
        }

        return conditionCode;
    }

    private void runQueryTask(ExtractionRequest extractionRequest) {
        try {
            final String databaseName = extractionRequest.getDatabaseName();
            final List<QueryTask> queryTaskList = extractionRequest.getQueryTaskList();
            final TrRequestInfo requestInfo = extractionRequest.getRequestInfo();
            final Integer dataSetUID = requestInfo.getDataSetUID();
            final int queryTaskListSize = queryTaskList.size();

            for (int i = 0; i < queryTaskListSize; i++) {
                final QueryTask queryTask = queryTaskList.get(i);
                final TableCreationTask tableCreationTask = queryTask.getTableCreationTask();
                final DataExtractionTask dataExtractionTask = queryTask.getDataExtractionTask();

                final Long queryBeginTime = System.currentTimeMillis();
                logger.info(String.format("(dataSetUID=%d / threadName=%s) - Processing %d/%d query.", dataSetUID, currentThreadName, (i + 1), queryTaskListSize));

                if (tableCreationTask != null) {
                    logger.info(String.format("(dataSetUID=%d / threadName=%s) - Start table creation at Hive Query: %s", dataSetUID, currentThreadName, tableCreationTask.getQuery()));
                    rawDataDBService.createTable(tableCreationTask);
                }

                if (dataExtractionTask != null) {
                    logger.info(String.format("(dataSetUID=%d / threadName=%s) - Start data extraction at Hive Query: %s", dataSetUID, currentThreadName, dataExtractionTask.getQuery()));

                    final String dataFileName = dataExtractionTask.getDataFileName();
                    final String hdfsLocation = dataExtractionTask.getHdfsLocation();
                    final String header = dataExtractionTask.getHeader();
                    //
                    // TODO: Call REST API For Statistic
                    //
//                    try {
//                        if (tableCreationTask != null) {
//                            String[] dbAndTableName = tableCreationTask.getDbAndHashedTableName().split("[.]");
//                            statisticAPICaller.callCreateStatistic(requestInfo.getDataSetUID(), dbAndTableName[0], dbAndTableName[1]);
//                        }
//                    } catch (Exception e) {
//                        logger.warn(String.format("%s - Exception occurs at Statistic API Caller: %s", currentThreadName, e.getMessage()));
//                    }

                    rawDataDBService.extractData(dataExtractionTask);

                    //
                    // TODO: Merge Reducer output files in HDFS, download merged file to local file system.
                    //
                    shellScriptResolver.runReducePartsMerger(dataSetUID, hdfsLocation, header, homePath, dataFileName, databaseName);
                }

                final Long queryEndTime = System.currentTimeMillis() - queryBeginTime;
                logger.info(String.format("(dataSetUID=%d / threadName=%s) - Finish Hive Query: %s, Elapsed time: %d ms", dataSetUID, currentThreadName, queryTask, queryEndTime));
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    private void runArchiveTask(ExtractionRequest extractionRequest) {
        try {
            final String databaseName = extractionRequest.getDatabaseName();
            final TrRequestInfo requestInfo = extractionRequest.getRequestInfo();
            final Integer dataSetUID = requestInfo.getDataSetUID();

            //
            // TODO: Archive the extracted data set and finally send the file to FTP server.
            //
            final String archiveFileName = String.format("%s_%s.tar.gz", requestInfo.getUserID(), String.valueOf(new Timestamp(System.currentTimeMillis()).getTime()));
            final String ftpLocation = String.format("/%s/%s", requestInfo.getUserID(), databaseName);

            final long archiveFileBeginTime = System.currentTimeMillis();
            logger.info(String.format("(dataSetUID=%d / threadName=%s) - Start archiving the extracted data set: %s", dataSetUID, currentThreadName, archiveFileName));
            shellScriptResolver.runArchiveExtractedDataSet(dataSetUID, archiveFileName, ftpLocation, homePath, databaseName);
            logger.info(String.format("(dataSetUID=%d / threadName=%s) - Finish archiving the extracted data set: %s, Elapsed time: %d ms", dataSetUID, currentThreadName, archiveFileName, (System.currentTimeMillis() - archiveFileBeginTime)));

            final String ftpURI = String.format("%s/%s", ftpLocation, archiveFileName);
            dataIntegrationPlatformAPICaller.callCreateFtpInfo(requestInfo.getDataSetUID(), requestInfo.getUserID(), ftpURI);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
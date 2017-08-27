package org.bigdatacenter.healthcarekihasadataextractor.api.caller;

public interface DataIntegrationPlatformAPICaller {
    Integer PROCESS_STATE_CODE_COMPLETED = 1;
    Integer PROCESS_STATE_CODE_PROCESSING = 2;
    Integer PROCESS_STATE_CODE_REJECTED = 3;

    void callUpdateJobStartTime(Integer dataSetUID, Long jobStartTime);

    void callUpdateJobEndTime(Integer dataSetUID, Long jobEndTime);

    void callUpdateElapsedTime(Integer dataSetUID, Long elapsedTime);

    void callUpdateProcessState(Integer dataSetUID, Integer processState);

    void callCreateFtpInfo(Integer dataSetUID, String userID, String ftpURI);

    String callReadProjectionNames(Integer dataSetUID, String tableName);
}

package org.bigdatacenter.healthcarekihasadataextractor.api.caller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

@Component
public class DataIntegrationPlatformAPICallerImpl implements DataIntegrationPlatformAPICaller {
    private static final Logger logger = LoggerFactory.getLogger(DataIntegrationPlatformAPICallerImpl.class);
    private static final String currentThreadName = Thread.currentThread().getName();
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private final RestTemplate restTemplate;
    private final HttpHeaders headers;

    @Value("${platform.rest.api.update.job-start-time}")
    private String updateJobStartTimeURL;

    @Value("${platform.rest.api.update.job-end-time}")
    private String updateJobEndTimeURL;

    @Value("${platform.rest.api.update.elapsed-time}")
    private String updateElapsedTimeURL;

    @Value("${platform.rest.api.update.process-state}")
    private String updateProcessStateURL;

    @Value("${platform.rest.api.create.ftp-info}")
    private String createFtpInfoURL;

    @Value("${platform.rest.api.read.projection-names}")
    private String readProjectionNamesURL;

    public DataIntegrationPlatformAPICallerImpl() {
        restTemplate = new RestTemplate();
        restTemplate.getMessageConverters().add(new MappingJackson2HttpMessageConverter());

        headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
    }

    @Override
    public void callUpdateJobStartTime(Integer dataSetUID, Long jobStartTime) {
        final MultiValueMap<String, String> parameters = new LinkedMultiValueMap<>();
        parameters.add("dataSetUID", String.valueOf(dataSetUID));
        parameters.add("jobStartTime", dateFormat.format(new Date(jobStartTime)));

        HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<>(parameters, headers);
        Integer response = restTemplate.postForObject(updateJobStartTimeURL, request, Integer.class);

        logger.info(String.format("%s - The jobStartTime column updated %d record(s).", currentThreadName, response));
    }

    @Override
    public void callUpdateJobEndTime(Integer dataSetUID, Long jobEndTime) {
        final MultiValueMap<String, String> parameters = new LinkedMultiValueMap<>();
        parameters.add("dataSetUID", String.valueOf(dataSetUID));
        parameters.add("jobEndTime", dateFormat.format(new Date(jobEndTime)));

        HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<>(parameters, headers);
        Integer response = restTemplate.postForObject(updateJobEndTimeURL, request, Integer.class);

        logger.info(String.format("%s - The jobEndTime column updated %d record(s).", currentThreadName, response));
    }

    @Override
    public void callUpdateElapsedTime(Integer dataSetUID, Long elapsedTime) {
        final String formattedElapsedTime = String.format("%02d:%02d:%02d",
                TimeUnit.MILLISECONDS.toHours(elapsedTime),
                TimeUnit.MILLISECONDS.toMinutes(elapsedTime) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(elapsedTime)),
                TimeUnit.MILLISECONDS.toSeconds(elapsedTime) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(elapsedTime)));

        final MultiValueMap<String, String> parameters = new LinkedMultiValueMap<>();
        parameters.add("dataSetUID", String.valueOf(dataSetUID));
        parameters.add("elapsedTime", formattedElapsedTime);

        HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<>(parameters, headers);
        Integer response = restTemplate.postForObject(updateElapsedTimeURL, request, Integer.class);

        logger.info(String.format("%s - The elapsedTime column updated %d record(s).", currentThreadName, response));
    }

    @Override
    public void callUpdateProcessState(Integer dataSetUID, Integer processState) {
        final MultiValueMap<String, String> parameters = new LinkedMultiValueMap<>();
        parameters.add("dataSetUID", String.valueOf(dataSetUID));
        parameters.add("processState", String.valueOf(processState));

        HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<>(parameters, headers);
        Integer response = restTemplate.postForObject(updateProcessStateURL, request, Integer.class);

        logger.info(String.format("%s - The processState column updated %d record(s).", currentThreadName, response));
    }

    @Override
    public void callCreateFtpInfo(Integer dataSetUID, String userID, String ftpURI) {
        final MultiValueMap<String, String> parameters = new LinkedMultiValueMap<>();
        parameters.add("dataSetUID", String.valueOf(dataSetUID));
        parameters.add("userID", userID);
        parameters.add("ftpURI", ftpURI);

        HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<>(parameters, headers);
        Integer response = restTemplate.postForObject(createFtpInfoURL, request, Integer.class);

        logger.info(String.format("%s - The FTP Info has been inserted. (code: %d)", currentThreadName, response));
    }

    @Override
    public String callReadProjectionNames(Integer dataSetUID, String tableName, Integer tableYear) {
        final MultiValueMap<String, String> parameters = new LinkedMultiValueMap<>();
        parameters.add("dataSetUID", String.valueOf(dataSetUID));
        parameters.add("tableName", tableName);
        parameters.add("tableYear", String.valueOf(tableYear));

        HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<>(parameters, headers);

        return restTemplate.postForObject(readProjectionNamesURL, request, String.class);
    }
}
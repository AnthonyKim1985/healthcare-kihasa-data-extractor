package org.bigdatacenter.healthcarekihasadataextractor.api.caller.statistic;

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

@Component
public class StatisticAPICallerImpl implements StatisticAPICaller {
    private static final Logger logger = LoggerFactory.getLogger(StatisticAPICallerImpl.class);
    private static final String currentThreadName = Thread.currentThread().getName();

    private final RestTemplate restTemplate;

    private final HttpHeaders headers;

    @Value("${platform.rest.api.create.statistic}")
    private String createStatisticURL;

    public StatisticAPICallerImpl() {
        restTemplate = new RestTemplate();
        restTemplate.getMessageConverters().add(new MappingJackson2HttpMessageConverter());

        headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
    }

    @Override
    public void callCreateStatistic(Integer dataSetUID, String databaseName, String tableName) {
        final MultiValueMap<String, String> parameters = new LinkedMultiValueMap<>();
        parameters.add("dataSetUID", String.valueOf(dataSetUID));
        parameters.add("DB", databaseName);
        parameters.add("TABLE", tableName);

        final HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<>(parameters, headers);
        final String response = restTemplate.postForObject(createStatisticURL, request, String.class);

        logger.info(String.format("(dataSetUID=%d / threadName=%s) - Response From Statistic REST API Server: %s", dataSetUID, currentThreadName, response));
    }
}
package org.bigdatacenter.healthcarekihasadataextractor.api;

import com.google.gson.Gson;
import org.bigdatacenter.healthcarekihasadataextractor.api.caller.DataIntegrationPlatformAPICaller;
import org.bigdatacenter.healthcarekihasadataextractor.config.RabbitMQConfig;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.parameter.ExtractionParameter;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.ExtractionRequest;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.response.ExtractionResponse;
import org.bigdatacenter.healthcarekihasadataextractor.exception.RESTException;
import org.bigdatacenter.healthcarekihasadataextractor.resolver.extraction.ExtractionRequestResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletResponse;
import java.text.SimpleDateFormat;
import java.util.Date;

@RestController
@RequestMapping("/extraction/api")
public class DataExtractionController {
    private static final Logger logger = LoggerFactory.getLogger(DataExtractionController.class);
    private static final String currentThreadName = Thread.currentThread().getName();
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private final RabbitTemplate rabbitTemplate;

    private final ExtractionRequestResolver extractionRequestResolver;

    private final DataIntegrationPlatformAPICaller dataIntegrationPlatformAPICaller;

    @Autowired
    public DataExtractionController(ExtractionRequestResolver extractionRequestResolver, RabbitTemplate rabbitTemplate, DataIntegrationPlatformAPICaller dataIntegrationPlatformAPICaller) {
        this.extractionRequestResolver = extractionRequestResolver;
        this.rabbitTemplate = rabbitTemplate;
        this.dataIntegrationPlatformAPICaller = dataIntegrationPlatformAPICaller;
    }

    @ResponseBody
    @ResponseStatus(HttpStatus.OK)
    @RequestMapping(value = "dataExtraction", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ExtractionResponse dataExtraction(@RequestBody ExtractionParameter extractionParameter, HttpServletResponse httpServletResponse) {
        final ExtractionRequest extractionRequest;
        final ExtractionResponse extractionResponse;
        final Integer dataSetUID  = extractionParameter.getRequestInfo().getDataSetUID();
        try {
            logger.info(String.format("%s - extractionParameter: %s", currentThreadName, extractionParameter));
            extractionRequest = extractionRequestResolver.buildExtractionRequest(extractionParameter);

            synchronized (this) {
                rabbitTemplate.convertAndSend(RabbitMQConfig.EXTRACTION_REQUEST_QUEUE, extractionRequest);
            }

            final String jobAcceptedTime = dateFormat.format(new Date(System.currentTimeMillis()));
            final String jsonForExtractionRequest = new Gson().toJson(extractionRequest, ExtractionRequest.class);
            extractionResponse = new ExtractionResponse(jobAcceptedTime, jsonForExtractionRequest);
        } catch (Exception e) {
            e.printStackTrace();
            dataIntegrationPlatformAPICaller.callUpdateProcessState(dataSetUID, DataIntegrationPlatformAPICaller.PROCESS_STATE_CODE_REJECTED);
            throw new RESTException(String.format("Bad request (%s)", e.getMessage()), httpServletResponse);
        }

        return extractionResponse;
    }
}
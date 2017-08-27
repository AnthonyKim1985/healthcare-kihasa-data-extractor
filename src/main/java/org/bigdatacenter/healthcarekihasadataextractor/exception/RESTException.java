package org.bigdatacenter.healthcarekihasadataextractor.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletResponse;

public class RESTException extends RuntimeException {
    private static final Logger logger = LoggerFactory.getLogger(RESTException.class);
    private final String currentThreadName = Thread.currentThread().getName();

    public RESTException(String message) {
        super(message);
        logger.error(String.format("%s - REST Exception occurs: %s", currentThreadName, message));
    }

    public RESTException(Throwable cause) {
        super(cause);
        logger.error(String.format("%s - REST Exception occurs: %s", currentThreadName, cause.getMessage()));
    }

    public RESTException(String message, Throwable cause) {
        super(message, cause);
        logger.error(String.format("%s - REST Exception occurs: %s, caused: %s", currentThreadName, message, cause.getMessage()));
    }

    public RESTException(String message, HttpServletResponse httpServletResponse) {
        super(message);
        logger.error(String.format("%s - REST Exception occurs: %s", currentThreadName, message));
        httpServletResponse.setStatus(HttpServletResponse.SC_BAD_REQUEST);
    }
}
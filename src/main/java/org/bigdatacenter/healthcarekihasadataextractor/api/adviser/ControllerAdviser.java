package org.bigdatacenter.healthcarekihasadataextractor.api.adviser;

import org.bigdatacenter.healthcarekihasadataextractor.exception.RESTException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@ControllerAdvice
public class ControllerAdviser {
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler(value = RESTException.class)
    public String handleBaseException(RESTException e) {
        return e.getMessage();
    }

    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler(value = Exception.class)
    public String handleException(Exception e) {
        return e.getMessage();
    }
}

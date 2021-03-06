package org.bigdatacenter.healthcarekihasadataextractor.resolver.extraction.parameter;

import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.parameter.ExtractionParameter;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.parameter.ExtractionRequestParameter;

public interface ExtractionRequestParameterResolver {
    ExtractionRequestParameter buildRequestParameter(ExtractionParameter extractionParameter);
}

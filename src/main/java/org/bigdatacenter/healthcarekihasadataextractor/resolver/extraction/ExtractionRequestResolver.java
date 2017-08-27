package org.bigdatacenter.healthcarekihasadataextractor.resolver.extraction;

import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.parameter.ExtractionParameter;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.ExtractionRequest;

public interface ExtractionRequestResolver {
    ExtractionRequest buildExtractionRequest(ExtractionParameter extractionParameter);
}

package org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.parameter;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.parameter.info.AdjacentTableInfo;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.parameter.map.ParameterKey;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.parameter.map.ParameterValue;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ExtractionRequestParameter {
    private Map<Integer/* Year */, Set<ParameterKey>> yearJoinKeyMap;
    private Map<Integer/* Year */, Map<ParameterKey, List<ParameterValue>>> yearParameterMap;
    private Map<Integer/* Year */, Set<AdjacentTableInfo>> yearAdjacentTableInfoMap;
}

package org.bigdatacenter.healthcarekihasadataextractor.domain.transaction;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TrRequestInfo implements Serializable {
    private Integer dataSetUID;
    private Integer dataSetFID;
    private String siteID;
    private Integer menuID;
    private Integer subMenuID;
    private Integer lowID;
    private Integer datasetID;
    private String datasetCategory;
    private String datasetSubject;
    private String datasetContents;
    private String datasetTag;
    private String userID;
    private String userName;
    private String userEmail;
    private String managerName;
    private String managerEmail;
    private Integer linkID;
    private String signDate;
    private String processDate;
    private Integer dataState;
    private Integer processState;
    private Integer openState;
    private Integer dataType;
    private Integer whoState;
    private String startDate;
    private String endDate;
    private Integer ranking;
    private String versionInfo;
    private Integer delState;
    private String jobStartTime;
    private String jobEndTime;
    private String elapsedTime;
    private String joinCondition;
    private Integer joinConditionYear;
    private Integer affy5MapNumber;
    private Integer groupUID;
    private Integer orgID;
}

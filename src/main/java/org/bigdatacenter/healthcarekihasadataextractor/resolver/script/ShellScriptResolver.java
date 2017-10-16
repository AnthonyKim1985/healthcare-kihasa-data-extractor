package org.bigdatacenter.healthcarekihasadataextractor.resolver.script;

public interface ShellScriptResolver {
    void runReducePartsMerger(Integer dataSetUID, String hdfsLocation, String header, String homePath, String dataFileName, String dataSetName);

    void runArchiveExtractedDataSet(Integer dataSetUID, String archiveFileName, String ftpLocation, String homePath, String dataSetName);
}

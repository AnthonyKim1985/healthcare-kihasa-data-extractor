package org.bigdatacenter.healthcarekihasadataextractor.resolver.script;

public interface ShellScriptResolver {
    void runReducePartsMerger(String hdfsLocation, String header, String homePath, String dataFileName, String dataSetName);

    void runArchiveExtractedDataSet(String archiveFileName, String ftpLocation, String homePath, String dataSetName);
}

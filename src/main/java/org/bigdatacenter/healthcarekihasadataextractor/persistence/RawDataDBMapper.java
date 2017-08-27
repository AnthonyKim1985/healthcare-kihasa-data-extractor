package org.bigdatacenter.healthcarekihasadataextractor.persistence;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.task.creation.TableCreationTask;
import org.bigdatacenter.healthcarekihasadataextractor.domain.extraction.request.task.extraction.DataExtractionTask;

@Mapper
public interface RawDataDBMapper {
    @Select("INSERT OVERWRITE DIRECTORY #{dataExtractionTask.hdfsLocation} ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ${dataExtractionTask.query}")
    void extractData(@Param("dataExtractionTask") DataExtractionTask dataExtractionTask);

    @Select("CREATE TABLE IF NOT EXISTS ${tableCreationTask.dbAndHashedTableName} AS ${tableCreationTask.query}")
    void createTable(@Param("tableCreationTask") TableCreationTask tableCreationTask);
}
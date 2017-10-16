package org.bigdatacenter.healthcarekihasadataextractor.resolver.script;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;

@Component
public class ShellScriptResolverImpl implements ShellScriptResolver {
    private static final Logger logger = LoggerFactory.getLogger(ShellScriptResolverImpl.class);

    @Override
    public void runReducePartsMerger(Integer dataSetUID, String hdfsLocation, String header, String homePath, String dataFileName, String dataSetName) {
        fork(dataSetUID, CommandBuilder.buildReducePartsMerger(hdfsLocation, header, homePath, dataFileName, dataSetName));
    }

    @Override
    public void runArchiveExtractedDataSet(Integer dataSetUID, String archiveFileName, String ftpLocation, String homePath, String dataSetName) {
        fork(dataSetUID, CommandBuilder.buildArchiveExtractedDataSet(archiveFileName, ftpLocation, homePath, dataSetName));
    }

    private void fork(Integer dataSetUID, String target) {
        try {
            Process process = Runtime.getRuntime().exec(target);

            final Thread stdinStreamResolver = new Thread(new InputStreamResolver(dataSetUID, "input_stream", process.getInputStream()));
            stdinStreamResolver.start();

            final Thread stderrStreamResolver = new Thread(new InputStreamResolver(dataSetUID, "error_stream", process.getErrorStream()));
            stderrStreamResolver.start();

            process.waitFor();
        } catch (IOException | InterruptedException e) {
            logger.warn(String.format("(dataSetUID=%d / threadName=%s) - Forked process occurs an exception: %s", dataSetUID, Thread.currentThread().getName(), e.getMessage()));
        }
    }

    private static final class CommandBuilder implements Serializable {
        static String buildReducePartsMerger(String hdfsLocation, String header, String homePath, String dataFileName, String dataSetName) {
            return String.format("sh sh/hdfs-parts-merger.sh %s %s %s %s %s", hdfsLocation, header, homePath, dataFileName, dataSetName);
        }

        static String buildArchiveExtractedDataSet(String archiveFileName, String ftpLocation, String homePath, String dataSetName) {
            return String.format("sh sh/archive-data-set.sh %s %s %s %s", archiveFileName, ftpLocation, homePath, dataSetName);
        }
    }

    @Data
    @AllArgsConstructor
    private final class InputStreamResolver implements Runnable, Serializable {
        private final Integer dataSetUID;
        private final String streamName;
        private final InputStream inputStream;

        @Override
        public void run() {
            final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");

            try (FileWriter fileWriter = new FileWriter(new File(String.format("logs/sh/%s_%s.log", simpleDateFormat.format(new Date()), streamName)), true);
                 BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))) {

                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    fileWriter.write(String.format("[%s] %s\n", new Date().toString(), line));
                    fileWriter.flush();
                }
            } catch (IOException e) {
                logger.warn(String.format("(dataSetUID=%d / threadName=%s) - Forked process occurs an exception: %s", dataSetUID, Thread.currentThread().getName(), e.getMessage()));
            }
        }
    }
}
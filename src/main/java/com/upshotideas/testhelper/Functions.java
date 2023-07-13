package com.upshotideas.testhelper;

import com.upshotideas.testhelper.filereader.IOperatingMode;
import com.upshotideas.testhelper.filereader.OperatingModeFactory;
import org.apache.commons.io.FilenameUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Functions for internal use.
 */
public class Functions {

    /**
     * Uses the operating mode to convert the identified list of files into insert statements.
     */
    private static final Pattern FILE_ORDER = Pattern.compile("^(\\d+)\\.");
    static Map<String, CopyOperation> generateTableSqls(LinkedHashMap<String, Path> orderedFiles, OperatingMode operatingMode) {
        IOperatingMode operatingModeHandler = OperatingModeFactory.getOperatingModeHandler(operatingMode);
        return orderedFiles.entrySet().stream().map((Map.Entry<String, Path> e) -> {
            try {
                return operatingModeHandler.generateTableSql(e);
            } catch (IOException ex) {
                throw new TestDataLoaderException(ex);
            }
        }).collect(Collectors.toMap(
                kv -> kv.tableName,
                kv -> kv.operation,
                (k1, k2) -> k1,
                LinkedHashMap::new
        ));
    }

    static LinkedHashMap<String, Path> getOrderedListOfFiles(String dataPath) {
        try (Stream<Path> filesStream = Files.walk(Paths.get(dataPath))) {
            return filesStream
                    .filter(p -> Files.isRegularFile(p) && FilenameUtils.isExtension(String.valueOf(p.getFileName()), "csv"))
                    .sorted(prefixNumericComparatorGenerator())
                    .collect(Collectors.toMap(path -> {
                                String fileName = path.getFileName().toString();
                                return fileName.replaceAll(String.valueOf(FILE_ORDER), "")
                                        .replaceAll(".csv", "");
                            }, path -> path,
                            (k1, k2) -> k1,
                            LinkedHashMap::new
                    ));
        } catch (IOException e) {
            throw new TestDataLoaderException(e);
        }
    }

    static Comparator<Path> prefixNumericComparatorGenerator() {
        return Comparator.comparing(path -> {
            String fileName = path.getFileName().toString();
            Matcher matchedName = FILE_ORDER.matcher(fileName);
            if (matchedName.find()) {
                return Integer.valueOf(matchedName.group(1));
            } else {
                return Integer.MAX_VALUE;
            }
        });
    }

    public static TableOperationTuple getNoopOperation(String tableName) {
        return new TableOperationTuple(tableName, connection -> {
            // noop
        });
    }

    private Functions() {
        // blocking construction
    }
}

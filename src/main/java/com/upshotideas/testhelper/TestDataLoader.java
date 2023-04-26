package com.upshotideas.testhelper;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.reading.CsvReader;
import io.deephaven.csv.sinks.SinkFactory;
import io.deephaven.csv.util.CsvReaderException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class TestDataLoader {
    private static final String DEFAULT_DATA_PATH = "src/test/resources/data";
    private static final Pattern FILE_ORDER = Pattern.compile("^(\\d+)\\.");
    private final Connection connection;
    private String dataPath = DEFAULT_DATA_PATH;

    private Map<String, String> tableSqls = Collections.emptyMap();

    public TestDataLoader(Connection connection, String dataPath) {
        this.connection = connection;
        this.dataPath = dataPath;

        LinkedHashMap<String, Path> orderedFiles = getOrderedListOfFiles();
        this.tableSqls = generateTableSqls(orderedFiles);
    }
    public TestDataLoader(Connection connection) {
        this(connection, DEFAULT_DATA_PATH);
    }

    public void loadTables() {
        loadData(this.tableSqls.entrySet().stream());
    }

    private void loadData(Stream<Map.Entry<String, String>> tables) {
        tables.forEach(entry -> {
            try {
                this.connection.createStatement().executeUpdate(entry.getValue());
                this.connection.commit();
            } catch (SQLException e) {
                throw new TestDataLoaderException(e);
            }
        });
    }

    public void loadTables(List<String> tables) {
        if (tables.isEmpty()) {
            loadTables();
        } else {
            Stream<Map.Entry<String, String>> filteredTables = this.tableSqls.entrySet().stream()
                    .filter(e -> tables.contains(e.getKey()));
            loadData(filteredTables);
        }
    }

    public void clearTables() {
        ArrayList<Map.Entry<String, String>> entries = new ArrayList<>(this.tableSqls.entrySet());
        Collections.reverse(entries);
        Stream<Map.Entry<String, String>> tables = entries.stream();
        clearData(tables);
    }

    private void clearData(Stream<Map.Entry<String, String>> tables) {
        tables.forEach(entry -> {
            try {
                this.connection.createStatement().executeUpdate("delete from " + entry.getKey() + ";");
                this.connection.commit();
            } catch (SQLException e) {
                throw new TestDataLoaderException(e);
            }
        });
    }

    public void clearTables(List<String> tables) {
        if (tables.isEmpty()) {
            clearTables();
        } else {
            Stream<Map.Entry<String, String>> filteredTables = this.tableSqls.entrySet().stream()
                    .filter(e -> tables.contains(e.getKey()));
            clearData(filteredTables);
        }
    }

    public void reloadTables() {
        this.clearTables();
        this.loadTables();
    }

    public void reloadTables(List<String> tables) {
        if (tables.isEmpty()) {
            reloadTables();
        } else {
            this.clearTables(tables);
            this.loadTables(tables);
        }
    }

    private Map<String, String> generateTableSqls(LinkedHashMap<String, Path> orderedFiles) {
        return orderedFiles.entrySet().stream().map((Map.Entry<String, Path> e) -> {
            try {
                Optional<String> optionalCols = Files.lines(e.getValue()).findFirst();
                String cols = "";
                if (optionalCols.isPresent()) {
                    cols = optionalCols.get();
                } else {
                    return Arrays.asList(e.getKey(), "");
                }
                return Arrays.asList(e.getKey(), "insert into " + e.getKey() + "(" + cols + ") select * from CSVREAD('" + e.getValue() + "',null,'charset=UTF-8');");
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }).collect(Collectors.toMap(
                kv -> kv.get(0),
                kv -> kv.get(1),
                (k1, k2) -> k1,
                LinkedHashMap::new
        ));
    }

//    private Map<String, String> generateTableSqls(LinkedHashMap<String, Path> orderedFiles) {
//        return orderedFiles.entrySet().stream().map((Map.Entry<String, Path> e) -> {
//            try {
//                List<String> fileLines = readFileLines(e);
//                if (fileLines.isEmpty()) {
//                    return Arrays.asList(e.getKey(), "");
//                }
//
//                String insertStmt = formInsertStatement(e.getKey(), fileLines);
//                return Arrays.asList(e.getKey(), insertStmt);
//            } catch (IOException ex) {
//                throw new TestDataLoaderException(ex);
//            }
//        }).collect(Collectors.toMap(
//                kv -> kv.get(0),
//                kv -> kv.get(1),
//                (k1, k2) -> k1,
//                LinkedHashMap::new
//        ));
//    }

/* TODO: why is this commented?

    // This doesnt really help, we still need to detect the type after the quote/dynmic part of the sql is removed.
    private static List<String> readFileLines(Map.Entry<String, Path> e) throws IOException {
        try (final InputStream inputStream = Files.newInputStream(e.getValue());) {
            final CsvSpecs specs = CsvSpecs.csv();
            final CsvReader.Result result = CsvReader.read(specs, inputStream, SinkFactory.arrays());
            final long rowCount = result.numRows();
            final long colCount = result.numCols();
            final List<List<String>> table = new ArrayList();

            StreamSupport.stream(result.spliterator(), false)
                    .map(resultColumn -> {
                        switch (resultColumn.dataType()) {
                            case STRING:
                                return quoteVal(String.valueOf(resultColumn.data()));
                        }
                    });

            for (CsvReader.ResultColumn col : result) {

            }

        } catch (CsvReaderException ex) {
            throw new RuntimeException(ex);
        }

//        try (CSVParser csvParser = new CSVParser(new FileReader(e.getValue().toFile()), CSVFormat.DEFAULT);) {
//            List<CSVRecord> fileLineRecords = csvParser.stream().collect(Collectors.toList());
//            return fileLineRecords.stream().map(csvRecord -> {
//                List<String> columns = csvRecord.stream()
//                        .map(String::trim)
//                        .map(TestDataLoader::quoteVal)
//                        .collect(Collectors.toList());
//                return String.join(",", columns);
//            }).collect(Collectors.toList());
//        }
    }


    private static String quoteVal(String s) {
        String operableCol = s.replaceAll("\"?__sql__\"?", "");
        if (!StringUtils.isNumeric(operableCol)) {
            return operableCol.replaceAll("^\"", "'")
                    .replaceAll("\"$", "'");
        }
        return operableCol;
    }
*/
//    private static List<String> readFileLines(Map.Entry<String, Path> e) throws IOException {
//        return FileUtils.readLines(e.getValue().toFile(), Charset.defaultCharset());
////        try (CSVParser csvParser = new CSVParser(new FileReader(e.getValue().toFile()), CSVFormat.DEFAULT);) {
////            List<CSVRecord> fileLineRecords = csvParser.stream().collect(Collectors.toList());
////            return fileLineRecords.stream().map(csvRecord -> {
////                List<String> columns = csvRecord.stream()
////                        .map(String::trim)
////                        .map(TestDataLoader::quoteVal)
////                        .collect(Collectors.toList());
////                return String.join(",", columns);
////            }).collect(Collectors.toList());
////        }
//    }
//
////    private static String quoteVal(String s) {
////        String operableCol = s.replaceAll("\"?__sql__\"?", "");
////        if (!StringUtils.isNumeric(operableCol)) {
////            return operableCol.replaceAll("^\"", "'")
////                    .replaceAll("\"$", "'");
////        }
////        return operableCol;
////    }

    private static String formInsertStatement(String tableName, List<String> fileLines) {
        String columns = fileLines.remove(0);

        List<String> inserts = fileLines.stream()
                .map(s -> s.replaceAll("\"?__sql__\"?", "")
                        .replaceAll("(?<!\")\"(?!\")", "'")
                        .replaceAll("\"\"", "\"")
                )
                .map(row -> String.format("insert into %s(%s) values (%s);", tableName, columns, row))
                .collect(Collectors.toList());

        return String.join("", inserts);
    }

    private LinkedHashMap<String, Path> getOrderedListOfFiles() {
        try (Stream<Path> filesStream = Files.walk(Paths.get(this.dataPath))) {
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

    private static Comparator<Path> prefixNumericComparatorGenerator() {
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
}

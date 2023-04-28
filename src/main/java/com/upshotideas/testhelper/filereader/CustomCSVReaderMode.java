package com.upshotideas.testhelper.filereader;

import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This mode allows for some dynamic components to the CSV. One can embed SQL snippets in between __sql__ tags and 
 * the generated insert statements will be such that they allow for executing these sqls, enabling some data to be
 * dynamic.
 * <p/>
 * Since this is a custom method, there are some constraints this puts on the data's format; the data needs to be
 * properly formatted, always enclosed in double quotes, if it is of type string.
 */
class CustomCSVReaderMode implements IOperatingMode {
    @Override
    public List<String> generateTableSql(Map.Entry<String, Path> e) throws IOException {
        List<String> fileLines = FileUtils.readLines(e.getValue().toFile(), Charset.defaultCharset());
        if (fileLines.isEmpty()) {
            return Arrays.asList(e.getKey(), "");
        }

        String insertStmt = formInsertStatement(e.getKey(), fileLines);
        return Arrays.asList(e.getKey(), insertStmt);
    }

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
}
